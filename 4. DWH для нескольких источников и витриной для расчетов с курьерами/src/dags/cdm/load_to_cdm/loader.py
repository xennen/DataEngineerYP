from logging import Logger

from lib import PgConnect
from cdm.load_to_cdm.origin_repository import OriginRepository
from cdm.load_to_cdm.dest_repository import DestRepository

class Loader:
    
    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OriginRepository(pg_dest)
        self.stg = DestRepository()
        self.log = log

    def load_settlements_reports(self):
        with self.pg_dest.connection() as conn:

            load_queue = self.origin.list_settlement_report()
            self.log.info(f"Found {len(load_queue)} Settlement reports to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for report in load_queue:
                self.stg.insert_settlement_report(conn, report)
    
    def load_courier_ledger_reports(self):
        with self.pg_dest.connection() as conn:

            load_queue = self.origin.list_courier_ledger_report()
            self.log.info(f"Found {len(load_queue)} Courier Ledger reports to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for report in load_queue:
                self.stg.insert_courier_ledger_report(conn, report)
