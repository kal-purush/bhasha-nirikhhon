# -*- coding: utf-8 -*-

import os
import sys
import fcntl
import signal
import logging.config

from ConfigParser import ConfigParser

from openprocurement.search.engine import IndexEngine, logger

from openprocurement.search.index.orgs import OrgsIndex
from openprocurement.search.source.orgs import OrgsSource
from openprocurement.search.source.tender import TenderSource
from openprocurement.search.source.ocds import OcdsSource
from openprocurement.search.source.plan import PlanSource
from munch import munchify


engine = type('engine', (), {})()

def sigterm_handler(signo, frame):
    logger.warning("Signal received %d", signo)
    engine.should_exit = True
    signal.alarm(3)
    sys.exit(0)


class IndexOrgsEngine(IndexEngine):
    def __init__(self, config):
        super(IndexOrgsEngine, self).__init__(config)
        self.orgs_map = {}

    def process_entity(self, entity):
        code = entity.get('identifier', {}).get('id', None)
        if not code or len(code) < 5 or len(code) > 15:
            return
        try:
            self.orgs_map[code] += 1
        except KeyError:
            self.orgs_map[code] = 1

    def process_source(self, source):
        logger.info("Process source [%s]", source.doc_type)
        items_list = True
        items_count = 0
        while items_list:
            if self.should_exit:
                break
            items_list = source.items()
            for meta in items_list:
                if self.should_exit:
                    break
                items_count += 1
                item = source.get(meta)
                entity = source.procuring_entity(item)
                if entity:
                    self.process_entity(entity)
            logger.info("[%s] Processed %d last %s map_size %d",
                source.doc_type, items_count,
                meta.get('dateModified'), len(self.orgs_map))

    def flush_orgs_map(self):
        index_name = self.get_current_indexes()
        logger.info("Flush orgs to index [%s]", index_name)
        if not index_name:
            return
        iter_count = 0
        create_count = 0
        update_count = 0
        orgs_index = self.index_list[0]
        doc_type = orgs_index.source.doc_type
        map_len = len(self.orgs_map)
        for code,rank in self.orgs_map.iteritems():
            if self.should_exit:
                break
            iter_count += 1
            if iter_count % 100 == 0:
                logger.info("[%s] Created %d updated %d orgs %d%%",
                    index_name, create_count, update_count,
                    int(100*iter_count/map_len))
            # get item
            meta = {'id': code, 'doc_type': doc_type}
            found = self.get_item(index_name, meta)
            # if not found - create
            if found:
                # if rank not changed - ignore
                if found['_source']['rank'] == rank:
                    continue
                item = {
                    'meta': {
                        'id': found['_id'],
                        'doc_type': found['_type'],
                        'version': found['_version']+1,
                    },
                    'data': found['_source'],
                }
                update_count += 1
            else:
                create_count += 1
                meta['version'] = 1
                item = orgs_index.source.patch_item(meta)
                if not item['data']['name']:
                    #logger.warning("Noname for %s", item['meta']['id'])
                    continue
            item['data']['rank'] = rank
            self.index_item(index_name, item)


def main():
    if len(sys.argv) < 2:
        print("usage: update_orgs etc/search.ini")
        sys.exit(1)

    parser = ConfigParser()
    parser.read(sys.argv[1])
    config = dict(parser.items('search_engine'))

    logging.config.fileConfig(sys.argv[1])

    # try get exclusive lock to prevent second start
    lock_filename = config.get('update_orgs') or 'update_orgs.pid'
    lock_file = open(lock_filename, "w")
    fcntl.lockf(lock_file, fcntl.LOCK_EX+fcntl.LOCK_NB)
    lock_file.write(str(os.getpid())+"\n")
    lock_file.flush()

    signal.signal(signal.SIGTERM, sigterm_handler)
    #signal.signal(signal.SIGINT, sigterm_handler)

    try:
        global engine
        engine = IndexOrgsEngine(config)
        source = OrgsSource(config)
        OrgsIndex(engine, source, config)
        if config.get('api_url', None):
            source = TenderSource(config)
            engine.process_source(source)
        if config.get('ocds_dir', None):
            source = OcdsSource(config)
            engine.process_source(source)
        if config.get('plan_api_url', None):
            source = PlanSource(config)
            engine.process_source(source)
        engine.flush_orgs_map()
    except Exception as e:
        logger.exception("Exception: %s", str(e))
    finally:
        lock_file.close()
        os.remove(lock_filename)
        logger.info("Shutdown")

    return 0


if __name__ == "__main__":
    main()