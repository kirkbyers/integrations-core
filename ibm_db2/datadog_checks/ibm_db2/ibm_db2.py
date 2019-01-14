# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from __future__ import division

import ibm_db

from datadog_checks.base import AgentCheck
from datadog_checks.base.utils.containers import hash_mutable
from .utils import scrub_connection_string, status_to_service_check
from . import queries


class IbmDb2Check(AgentCheck):
    METRIC_PREFIX = 'ibm_db2'
    SERVICE_CHECK_CONNECT = '{}.can_connect'.format(METRIC_PREFIX)
    SERVICE_CHECK_STATUS = '{}.status'.format(METRIC_PREFIX)

    def __init__(self, name, init_config, agentConfig, instances=None):
        super(IbmDb2Check, self).__init__(name, init_config, agentConfig, instances)
        self._config_cache = {}

    def check(self, instance):
        config = self.get_config(instance)
        if config is None:
            return

        connection = config['connection']
        tags = config['tags']

        self.query_instance(connection, tags)
        self.query_database(connection, tags)
        self.query_buffer_pool(connection, tags)
        self.query_table_space(connection, tags)
        self.query_transaction_log(connection, tags)

    def query_instance(self, connection, tags):
        statement = ibm_db.exec_immediate(connection, queries.INSTANCE_TABLE)
        result = ibm_db.fetch_assoc(statement)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060773.html
        self.gauge(self.m('connection.active'), result['total_connections'], tags=tags)

    def query_database(self, connection, tags):
        statement = ibm_db.exec_immediate(connection, queries.DATABASE_TABLE)
        result = ibm_db.fetch_assoc(statement)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001156.html
        self.service_check(self.SERVICE_CHECK_STATUS, status_to_service_check(result['db_status']), tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001201.html
        self.gauge(self.m('application.active'), result['appls_cur_cons'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001202.html
        self.gauge(self.m('application.executing'), result['appls_in_db2'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0002225.html
        self.monotonic_count(self.m('connection.max'), result['connections_top'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001200.html
        self.monotonic_count(self.m('connection.total'), result['total_cons'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001283.html
        self.monotonic_count(self.m('lock.dead'), result['deadlocks'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001290.html
        self.monotonic_count(self.m('lock.timeouts'), result['lock_timeouts'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001281.html
        self.gauge(self.m('lock.active'), result['num_locks_held'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001296.html
        self.gauge(self.m('lock.waiting'), result['num_locks_waiting'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001294.html
        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001293.html
        if result['lock_waits']:
            average_lock_wait = result['lock_wait_time'] / result['lock_waits']
        else:
            average_lock_wait = 0
        self.gauge(self.m('lock.wait'), average_lock_wait, tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001282.html
        # https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.admin.config.doc/doc/r0000267.html
        self.gauge(self.m('lock.pages'), result['lock_list_in_use'] / 4096, tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001160.html
        last_backup = result['last_backup']
        if last_backup:
            seconds_since_last_backup = (result['current_time'] - last_backup).total_seconds()
        else:
            seconds_since_last_backup = -1
        self.gauge(self.m('backup.latest'), seconds_since_last_backup, tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0051568.html
        self.monotonic_count(self.m('row.modified.total'), result['rows_modified'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001317.html
        self.monotonic_count(self.m('row.reads.total'), result['rows_read'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0051569.html
        self.monotonic_count(self.m('row.returned.total'), result['rows_returned'], tags=tags)

    def query_buffer_pool(self, connection, tags):
        # Hit ratio formulas:
        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056871.html
        statement = ibm_db.exec_immediate(connection, queries.BUFFER_POOL_TABLE)

        bp = ibm_db.fetch_assoc(statement)
        while bp:
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0002256.html
            bp_tags = ['bufferpool:{}'.format(bp['bp_name'])]
            bp_tags.extend(tags)

            # Column-organized pages

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060858.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060874.html
            column_reads_physical = bp['pool_col_p_reads'] + bp['pool_temp_col_p_reads']
            self.monotonic_count(self.m('bufferpool.column.reads.physical'), column_reads_physical, tags=bp_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060763.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060873.html
            column_reads_logical = bp['pool_col_l_reads'] + bp['pool_temp_col_l_reads']
            self.monotonic_count(self.m('bufferpool.column.reads.logical'), column_reads_logical, tags=bp_tags)

            # Submit total
            self.monotonic_count(
                self.m('bufferpool.column.reads.total'), column_reads_physical + column_reads_logical, tags=bp_tags
            )

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060857.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060850.html
            column_pages_found = bp['pool_col_lbp_pages_found'] - bp['pool_async_col_lbp_pages_found']

            if column_reads_logical:
                column_hit_percent = column_pages_found / column_reads_logical * 100
            else:
                column_hit_percent = 0
            self.gauge(self.m('bufferpool.column.hit_percent'), column_hit_percent, tags=bp_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060855.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0060856.html
            group_column_reads_logical = bp['pool_col_gbp_l_reads'] or 0
            group_column_pages_found = group_column_reads_logical - (bp['pool_col_gbp_p_reads'] or 0)

            # Submit group ratio if in a pureScale environment
            if group_column_reads_logical:
                group_column_hit_percent = group_column_pages_found / group_column_reads_logical * 100
                self.gauge(self.m('bufferpool.group.column.hit_percent'), group_column_hit_percent, tags=bp_tags)

            # Data pages

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001236.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0011300.html
            data_reads_physical = bp['pool_data_p_reads'] + bp['pool_temp_data_p_reads']
            self.monotonic_count(self.m('bufferpool.data.reads.physical'), data_reads_physical, tags=bp_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001235.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0011302.html
            data_reads_logical = bp['pool_data_l_reads'] + bp['pool_temp_data_l_reads']
            self.monotonic_count(self.m('bufferpool.data.reads.logical'), data_reads_logical, tags=bp_tags)

            # Submit total
            self.monotonic_count(
                self.m('bufferpool.data.reads.total'), data_reads_physical + data_reads_logical, tags=bp_tags
            )

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056487.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056493.html
            data_pages_found = bp['pool_data_lbp_pages_found'] - bp['pool_async_data_lbp_pages_found']

            if data_reads_logical:
                data_hit_percent = data_pages_found / data_reads_logical * 100
            else:
                data_hit_percent = 0
            self.gauge(self.m('bufferpool.data.hit_percent'), data_hit_percent, tags=bp_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056485.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056486.html
            group_data_reads_logical = bp['pool_data_gbp_l_reads'] or 0
            group_data_pages_found = group_data_reads_logical - (bp['pool_data_gbp_p_reads'] or 0)

            # Submit group ratio if in a pureScale environment
            if group_data_reads_logical:
                group_data_hit_percent = group_data_pages_found / group_data_reads_logical * 100
                self.gauge(self.m('bufferpool.group.data.hit_percent'), group_data_hit_percent, tags=bp_tags)

            # Index pages

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001239.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0011301.html
            index_reads_physical = bp['pool_index_p_reads'] + bp['pool_temp_index_p_reads']
            self.monotonic_count(self.m('bufferpool.index.reads.physical'), index_reads_physical, tags=bp_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001238.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0011303.html
            index_reads_logical = bp['pool_index_l_reads'] + bp['pool_temp_index_l_reads']
            self.monotonic_count(self.m('bufferpool.index.reads.logical'), index_reads_logical, tags=bp_tags)

            # Submit total
            self.monotonic_count(
                self.m('bufferpool.index.reads.total'), index_reads_physical + index_reads_logical, tags=bp_tags
            )

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056243.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056496.html
            index_pages_found = bp['pool_index_lbp_pages_found'] - bp['pool_async_index_lbp_pages_found']

            if index_reads_logical:
                index_hit_percent = index_pages_found / index_reads_logical * 100
            else:
                index_hit_percent = 0
            self.gauge(self.m('bufferpool.index.hit_percent'), index_hit_percent, tags=bp_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056488.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0056489.html
            group_index_reads_logical = bp['pool_index_gbp_l_reads'] or 0
            group_index_pages_found = group_index_reads_logical - (bp['pool_index_gbp_p_reads'] or 0)

            # Submit group ratio if in a pureScale environment
            if group_index_reads_logical:
                group_index_hit_percent = group_index_pages_found / group_index_reads_logical * 100
                self.gauge(self.m('bufferpool.group.index.hit_percent'), group_index_hit_percent, tags=bp_tags)

            # XML storage object (XDA) pages

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0022730.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0022739.html
            xda_reads_physical = bp['pool_xda_p_reads'] + bp['pool_temp_xda_p_reads']
            self.monotonic_count(self.m('bufferpool.xda.reads.physical'), xda_reads_physical, tags=bp_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0022731.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0022738.html
            xda_reads_logical = bp['pool_xda_l_reads'] + bp['pool_temp_xda_l_reads']
            self.monotonic_count(self.m('bufferpool.xda.reads.logical'), xda_reads_logical, tags=bp_tags)

            # Submit total
            self.monotonic_count(
                self.m('bufferpool.xda.reads.total'), xda_reads_physical + xda_reads_logical, tags=bp_tags
            )

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0058666.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0058670.html
            xda_pages_found = bp['pool_xda_lbp_pages_found'] - bp['pool_async_xda_lbp_pages_found']

            if xda_reads_logical:
                xda_hit_percent = xda_pages_found / xda_reads_logical * 100
            else:
                xda_hit_percent = 0
            self.gauge(self.m('bufferpool.xda.hit_percent'), xda_hit_percent, tags=bp_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0058664.html
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0058665.html
            group_xda_reads_logical = bp['pool_xda_gbp_l_reads'] or 0
            group_xda_pages_found = group_xda_reads_logical - (bp['pool_xda_gbp_p_reads'] or 0)

            # Submit group ratio if in a pureScale environment
            if group_xda_reads_logical:
                group_xda_hit_percent = group_xda_pages_found / group_xda_reads_logical * 100
                self.gauge(self.m('bufferpool.group.xda.hit_percent'), group_xda_hit_percent, tags=bp_tags)

            # Compute overall stats
            reads_physical = column_reads_physical + data_reads_physical + index_reads_physical + xda_reads_physical
            self.monotonic_count(self.m('bufferpool.reads.physical'), reads_physical, tags=bp_tags)

            reads_logical = column_reads_logical + data_reads_logical + index_reads_logical + xda_reads_logical
            self.monotonic_count(self.m('bufferpool.reads.logical'), reads_logical, tags=bp_tags)

            reads_total = reads_physical + reads_logical
            self.monotonic_count(self.m('bufferpool.reads.total'), reads_total, tags=bp_tags)

            if reads_logical:
                pages_found = column_pages_found + data_pages_found + index_pages_found + xda_pages_found
                hit_percent = pages_found / reads_logical * 100
            else:
                hit_percent = 0
            self.gauge(self.m('bufferpool.hit_percent'), hit_percent, tags=bp_tags)

            # Submit group ratio if in a pureScale environment
            group_reads_logical = (
                group_column_reads_logical
                + group_data_reads_logical
                + group_index_reads_logical
                + group_xda_reads_logical
            )
            if group_reads_logical:
                group_pages_found = (
                    group_column_pages_found + group_data_pages_found + group_index_pages_found + group_xda_pages_found
                )
                group_hit_percent = group_pages_found / group_reads_logical * 100
                self.gauge(self.m('bufferpool.group.hit_percent'), group_hit_percent, tags=bp_tags)

            # Get next buffer pool, if any
            bp = ibm_db.fetch_assoc(statement)

    def query_table_space(self, connection, tags):
        # Utilization formulas:
        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.sql.rtn.doc/doc/r0056516.html
        statement = ibm_db.exec_immediate(connection, queries.TABLE_SPACE_TABLE)

        ts = ibm_db.fetch_assoc(statement)
        while ts:
            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001295.html
            ts_tags = ['tablespace:{}'.format(ts['tbsp_name'])]
            ts_tags.extend(tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0007534.html
            page_size = ts['tbsp_page_size']

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0007539.html
            total_pages = ts['tbsp_total_pages']
            self.gauge(self.m('tablespace.size'), total_pages * page_size, tags=ts_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0007540.html
            usable_pages = ts['tbsp_usable_pages']
            self.gauge(self.m('tablespace.usable'), usable_pages * page_size, tags=ts_tags)

            # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0007541.html
            used_pages = ts['tbsp_used_pages']
            self.gauge(self.m('tablespace.used'), used_pages * page_size, tags=ts_tags)

            # Percent utilized
            if usable_pages:
                utilized = used_pages / usable_pages * 100
            else:
                utilized = 0
            self.gauge(self.m('tablespace.utilized'), utilized, tags=ts_tags)

            # Get next table space, if any
            ts = ibm_db.fetch_assoc(statement)

    def query_transaction_log(self, connection, tags):
        statement = ibm_db.exec_immediate(connection, queries.TRANSACTION_LOG_TABLE)
        result = ibm_db.fetch_assoc(statement)

        # https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.admin.config.doc/doc/r0000239.html
        block_size = 4096

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0002530.html
        used = result['total_log_used']
        self.gauge(self.m('log.used'), used / block_size, tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0002531.html
        available = result['total_log_available']

        # Handle infinite log space
        if available == -1:
            utilized = 0
        else:
            utilized = used / available * 100
            available /= block_size

        self.gauge(self.m('log.available'), available, tags=tags)
        self.gauge(self.m('log.utilized'), utilized, tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001278.html
        self.monotonic_count(self.m('log.reads'), result['log_reads'], tags=tags)

        # https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.admin.mon.doc/doc/r0001279.html
        self.monotonic_count(self.m('log.writes'), result['log_writes'], tags=tags)

    def get_config(self, instance):
        instance_id = hash_mutable(instance)
        config = self._config_cache.get(instance_id)

        if config is None:
            config = {
                'db': instance.get('db', ''),
                'username': instance.get('username', ''),
                'password': instance.get('password', ''),
                'host': instance.get('host', ''),
                'port': instance.get('port', 5000),
                'tags': instance.get('tags', []),
            }
            config['tags'].append('db:{}'.format(config['db']))

            config['connection'] = self.get_connection(config)
            if not config['connection']:
                return

            self._config_cache[instance_id] = config

        return config

    def get_connection(self, config):
        target, username, password = self.get_connection_data(config)

        # Get column names in lower case
        connection_options = {ibm_db.ATTR_CASE: ibm_db.CASE_LOWER}

        try:
            connection = ibm_db.connect(target, username, password, connection_options)
        except Exception as e:
            if config['host']:
                self.log.error('Unable to connect with `{}`: {}'.format(scrub_connection_string(target), e))
            else:
                self.log.error('Unable to connect to database `{}` as user `{}`: {}'.format(target, username, e))
            self.service_check(self.SERVICE_CHECK_CONNECT, self.CRITICAL, tags=config['tags'])
        else:
            self.service_check(self.SERVICE_CHECK_CONNECT, self.OK, tags=config['tags'])
            return connection

    @classmethod
    def get_connection_data(cls, config):
        if config['host']:
            target = 'database={};hostname={};port={};protocol=tcpip;uid={};pwd={}'.format(
                config['db'], config['host'], config['port'], config['username'], config['password']
            )
            username = ''
            password = ''
        else:
            target = config['db']
            username = config['username']
            password = config['password']

        return target, username, password

    @classmethod
    def m(cls, metric):
        return '{}.{}'.format(cls.METRIC_PREFIX, metric)
