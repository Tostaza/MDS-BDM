

import os
import json
import sys
import pandas as pd
from deltalake import DeltaTable, write_deltalake

class DeltaLake():

    def coonect_and_load(self, df_new, persistent_path, storage_options, predicate):
        try:
            dt = DeltaTable(persistent_path, storage_options=storage_options)
            (
                dt.merge(
                    source=df_new,
                    predicate=predicate,
                    source_alias="source",
                    target_alias="target"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            print(f"Data updated successfully. Merged {len(df_new)} records.")
        except Exception as e:
            print(f"Table not found. Initializing for the first time... (Details: {e})")
            write_deltalake(
                persistent_path,
                df_new,
                storage_options=storage_options,
                mode="overwrite"
            )
            print("Delta table initialized successfully.")
    
    def connect_load_specific_one(self, df_new, persistent_path, storage_options):
        try:
            write_deltalake(
                persistent_path,
                df_new,
                storage_options=storage_options,
                mode="append"
            )
            print("Data appended successfully.")

        except Exception as e:
            print(f"Table not found. Initializing... ({e})")

            write_deltalake(
                persistent_path,
                df_new,
                storage_options=storage_options,
                mode="overwrite"
            )