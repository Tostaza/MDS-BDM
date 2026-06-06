import time
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError

class DeltaLake():

    def _merge_delta(self, df_new, persistent_path, storage_options, predicate):
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

    def connect_and_load(self, df_new, persistent_path, storage_options, predicate, max_retries: int = 3):
        try:
            self._merge_delta(df_new, persistent_path, storage_options, predicate)
            print(f"Data updated successfully. Merged {len(df_new)} records.")
            return
        except TableNotFoundError:
            print("Table not found. Initializing for the first time...")
            # ``schema_mode="merge"`` només té efecte si la taula ja existeix; aquí es defensiu
            # per si una crida concurrent l'acaba de crear amb un esquema lleugerament diferent
            # (p. ex. Nitter afegint un camp al RSS entre runs successius).
            write_deltalake(
                persistent_path,
                df_new,
                storage_options=storage_options,
                mode="overwrite",
                schema_mode="merge",
            )
            print("Delta table initialized successfully.")

        except CommitFailedError as e:
            # Another writer committed first. Retry merge against latest table version.
            print(f"Concurrent transaction detected. Retrying merge... ({e})")
            for attempt in range(1, max_retries + 1):
                try:
                    time.sleep(min(2 ** attempt, 5))
                    self._merge_delta(df_new, persistent_path, storage_options, predicate)
                    print(
                        f"Data updated successfully after retry {attempt}. "
                        f"Merged {len(df_new)} records."
                    )
                    return
                except CommitFailedError as retry_error:
                    if attempt == max_retries:
                        raise retry_error
                    print(f"Retry {attempt} failed due to concurrency. Trying again...")
        except Exception:
            raise
    
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