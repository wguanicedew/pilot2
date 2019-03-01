#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018


from ess.client.client import Client

from .genericexecutor import GenericExecutor

import logging
logger = logging.getLogger(__name__)

"""
EventStreamingService Executor with one process to manage EventService
"""


class ESSExecutor(GenericExecutor):
    def __init__(self, **kwargs):
        super(ESSExecutor, self).__init__(**kwargs)
        self.setName("ESSExecutor")

        self.ess_client = Client(host='https://aipanda182.cern.ch:8443')
        self.avail_event_range_files = {}

    def download_event_range(self, event_range):
        payload = self.get_payload()
        workdir = payload['workdir']

        scope_name = '%s:%s' % (event_range['scope'], event_range['LFN'])
        if scope_name in self.avail_event_range_files:
            for file in self.avail_event_range_files[scope_name]:
                if file['startEvent'] <= event_range['startEvent'] and file['lastEvent'] >= event_range['lastEvent']:
                    logger.info('Local downloaded file %s already containt events for event range %s, will use it' % (file, event_range))

                    event_range['startEvent'] = event_range['startEvent'] - file['startEvent'] + 1
                    event_range['lastEvent'] = event_range['lastEvent'] - file['startEvent'] + 1
                    event_range['PFN'] = file['PFN']
                    logger.info("Update event range startEvent and lastEvent to be based on the local file: %s" % event_range)
                    return event_range

        logger.info("Calling ess client to download event stream file for: %s" % event_range)
        ret = self.ess_client.download(scope=event_range['scope'],
                                       name=event_range['LFN'],
                                       min_id=event_range['startEvent'],
                                       max_id=event_range['lastEvent'],
                                       dest_dir=workdir)
        if ret['status'] == 0:
            file = {'startEvent': ret['metadata']['min_id'],
                    'lastEvent': ret['metadata']['max_id'],
                    'PFN': ret['metadata']['name']}
            logger.info("Successfully downloaded file: %s" % file)
            if scope_name not in self.avail_event_range_files:
                self.avail_event_range_files[scope_name] = []
            self.avail_event_range_files[scope_name].append(file)

            event_range['startEvent'] = event_range['startEvent'] - file['startEvent'] + 1
            event_range['lastEvent'] = event_range['lastEvent'] - file['startEvent'] + 1
            event_range['PFN'] = file['PFN']
            logger.info("Update event range startEvent and lastEvent to be based on the local file: %s" % event_range)
            return event_range
        else:
            logger.info("Failed to download event range file for event range: %s" % (event_range))

    def get_event_ranges(self, num_event_ranges=1, queue_factor=2):
        logger.info("Getting event ranges: (num_ranges: %s)" % num_event_ranges)
        if len(self.event_ranges) < num_event_ranges:
            ret = self.communication_manager.get_event_ranges(num_event_ranges=num_event_ranges * queue_factor, job=self.get_job())
            for event_range in ret:
                event_range = self.download_event_range(event_range)
                if event_range:
                    self.event_ranges.append(event_range)

        ret = []
        for _ in range(num_event_ranges):
            if len(self.event_ranges) > 0:
                event_range = self.event_ranges.pop(0)
                ret.append(event_range)
        logger.info("Received event ranges(num:%s): %s" % (len(ret), ret))
        return ret
