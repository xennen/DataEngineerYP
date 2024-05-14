from logging import Logger

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from stg.bonus_system_dag.origin_repository import OriginRepository
from stg.bonus_system_dag.dest_repository import DestRepository

class Loader:

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OriginRepository(pg_origin)
        self.stg = DestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_events(self):
        WF_KEY = "events_origin_to_stg_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 1000
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_events(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} events to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for event in load_queue:
                self.stg.insert_event(conn, event)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
            
            
    def load_users(self):
        WF_KEY = "users_origin_to_stg_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 1000
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_users(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                self.stg.insert_user(conn, user)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
            
    
    def load_ranks(self):
        WF_KEY = "ranks_origin_to_stg_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 100
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_ranks(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for rank in load_queue:
                self.stg.insert_rank(conn, rank)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
