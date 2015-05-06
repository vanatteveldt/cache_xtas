import logging
import json
import time
from Queue import Queue, Empty
from threading import Thread, current_thread

from elasticsearch import Elasticsearch
from elasticsearch.client import indices
from xtas.tasks.es import es_document
from xtas.tasks.pipeline import pipeline
from xtas.tasks.single import corenlp_lemmatize

logging.basicConfig(format='[%(asctime)s %(levelname)s %(name)s:%(lineno)s %(threadName)s] %(message)s', level=logging.INFO)

def get_filter(setid, doctype):
    """Create a DSL filter dict to filter on set and no existing parser"""
    noparse =  {"not" : {"has_child" : { "type": doctype,
                                         "query" : {"match_all" : {}}}}}
    return {"bool" : {"must" : [{"term" : {"sets" : setid}}, noparse]}}

def get_articles(es, index, doctype, parent_doctype, setid, size=100):
    """Return one or more ranbom uncached articles from the set"""
    body = {"query" : {"function_score" : {"filter" : get_filter(setid, doctype), "random_score" : {}}}}
    result = es.search(index=index, doc_type=parent_doctype, body=body, fields=[], size=size)
    n = result['hits']['total']
    logging.warn("Fetched aids, {n} remaining".format(**locals()))
    return n, [int(r['_id']) for r in result['hits']['hits']]

def check_mapping(es, index, doctype, parent_doctype):
    """Check that the mapping for cached results of this plugin exists and create it otherwise"""
    logging.warn("Checking mapping {index}:{doctype} -> {parent_doctype}"
                 .format(**locals()))
    if not indices.IndicesClient(es).exists_type(index, doctype):
        logging.warn("Creating mapping {index}:{doctype} -> {parent_doctype}"
                     .format(**locals()))
        body = {doctype : {"_parent" : {"type" : "article"}}}
        indices.IndicesClient(es).put_mapping(index=index, doc_type=doctype, body=body)

def cache_many(pipe, docs, concurrency):
    docs_q = Queue()
    errors = {} # dict assignment is thread safe (right?)
    [docs_q.put(doc) for doc in docs]
    def cache():
        while True:
            try:
                doc = docs_q.get_nowait()
            except Empty:
                break
            try:
                logging.info("Proccesing {doc}, approx left: {n}"
                             .format(n=len(docs_q.queue), **locals()))
                pipeline(doc, pipe)
            except Exception, e:
                logging.warn("Error on processing {doc}: {e}".format(**locals()))
                errors[doc] = e
        logging.info("Worker done!")
    threads = [Thread(name="Worker_{n}".format(**locals()), target=cache)
               for n in range(concurrency)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return errors

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--host', default='amcat.vu.nl')
    parser.add_argument('--index', default='amcat')
    parser.add_argument('--parent-doctype', default='article' )
    parser.add_argument('--n', type=int, default=25)
    parser.add_argument('--concurrency', type=int, default=1)
    parser.add_argument('--norepeat', action='store_true')
    parser.add_argument('--eager', action='store_true')
    parser.add_argument('--single', action='store_true', help="Parse a single article: set argument is interpreted as article id")

    parser.add_argument('set', type=int)
    parser.add_argument('modules', nargs="+")

    args = parser.parse_args()

    from xtas.celery import app
    app.conf['CELERY_ALWAYS_EAGER'] = args.eager

    pipe = [{"module" : x} for x in args.modules]

    doctype = "__".join([args.parent_doctype] + [m['module'] for m in pipe])
    es = Elasticsearch(hosts=[{"host":args.host, "port": 9200}])
    check_mapping(es, args.index, doctype, args.parent_doctype)

    while True:
        if args.single:
            n, aids = 1, [args.set]
        else:
            logging.info("Retrieving {args.n} articles".format(**locals()))
            try:
                n, aids = list(get_articles(es, args.index, doctype, args.parent_doctype, args.set, size=args.n))
            except:
                logging.exception("Error on get_articles, retrying in 10 seconds")
                time.sleep(10)
                continue
        if not aids:
            logging.warn("DONE")
            break
        docs = [es_document(args.index, args.parent_doctype, aid, "text")
                for aid in aids]
        cache_many(pipe, docs, args.concurrency)

        if args.norepeat or args.single:
            break
