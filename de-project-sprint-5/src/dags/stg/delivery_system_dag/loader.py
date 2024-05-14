from datetime import datetime, timedelta
from logging import Logger

from stg import EtlSetting, StgEtlSettingsRepository
from stg.delivery_system_dag.pg_saver import PgSaver
from stg.delivery_system_dag.reader import Reader
from lib import PgConnect
from lib.dict_util import json2str


class Loader:
    _LOG_THRESHOLD = 2
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: Reader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger
        self.WF_KEY = f"deliverysystem_{collection_loader.collection}_to_stg"

    def run_copy(self) -> int:
        print(self.collection_loader.collection)
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_OFFSET_KEY: 0
                    }
                )

            last_loaded_offset = wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY]
            self.log.info(f"starting to load from last checkpoint: {last_loaded_offset}")

            load_queue = self.collection_loader.get_data(last_loaded_offset)
            self.log.info(f"Found {len(load_queue)} documents to sync from collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d[f"{self.collection_loader.id_field}"]), d["update_ts"], d['object_value'], self.collection_loader.collection)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing.")

            wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = last_loaded_offset + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
        
        
    def run_copy_delivery(self) -> int:
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
                    }
                )

            last_loaded_ts = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_data_delivery(last_loaded_ts)
            self.log.info(f"Found {len(load_queue)} documents to sync from collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d[f"{self.collection_loader.id_field}"]), d["update_ts"], d['object_value'], self.collection_loader.collection)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = datetime.strptime(max([t["update_ts"] for t in load_queue]), "%Y-%m-%d %H:%M:%S.%f").strftime('%Y-%m-%d %H:%M:%S')
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)