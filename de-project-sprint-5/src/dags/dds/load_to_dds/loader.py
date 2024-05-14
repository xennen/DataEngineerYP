from logging import Logger

from dds import EtlSetting, StgEtlSettingsRepository
from dds.load_to_dds.dest_repository import DestRepository
from dds.load_to_dds.origin_repository import OriginRepository
from lib import PgConnect
from lib.dict_util import json2str


class Loader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OriginRepository(pg_dest)
        self.dds = DestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        WF_KEY = "orders_stg_to_dds_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 10000
        
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order in load_queue:
                self.dds.query_orders(conn, order_obj=order)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
            
            
    def load_users(self):
        WF_KEY = "users_stg_to_dds_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 200
        
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
                self.dds.query_users(conn, user_obj=user)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
            
        
    def load_timestamps(self):
        WF_KEY = "timestamps_stg_to_dds_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 10000
        
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_timestamps(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for timestamp in load_queue:
                self.dds.query_timestamps(conn, ts_obj=timestamp)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
        
    
    def load_restaurants(self):
        WF_KEY = "restaurants_stg_to_dds_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 50
        
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_restaurants(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for restaurant in load_queue:
                self.dds.query_restaurants(conn, res_obj=restaurant)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
            
    
    def load_products(self):
        WF_KEY = "products_stg_to_dds_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 1000
        
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
 
            for product in load_queue:
                self.dds.query_products(conn, pr_obj=product)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
            
    
    def load_sales(self):
        WF_KEY = "sales_stg_to_dds_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 100
        
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_sales(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for sale in load_queue:
                self.dds.query_sales(conn, sales_obj=sale)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
        
    def load_couriers(self):
        WF_KEY = "couriers_stg_to_dds_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 100
        
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_couriers(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier in load_queue:
                self.dds.query_couriers(conn, courier_obj=courier)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")
            
    
    def load_deliveries(self):
        WF_KEY = "deliveries_stg_to_dds_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_id"
        BATCH_LIMIT = 100
        
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery in load_queue:
                self.dds.query_deliveries(conn, delivery_obj=delivery)

            wf_setting.workflow_settings[LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[LAST_LOADED_ID_KEY]}")