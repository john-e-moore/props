import unittest
import os
import pandas as pd
from handlers.duckdb_handler import DuckDBHandler

class TestDuckDBHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_path = 'test_duckdb.db'
        cls.duckdb_handler = DuckDBHandler(cls.db_path)
        cls.test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        cls.duckdb_handler.conn.execute("""
            CREATE TABLE test_table (
                id INTEGER,
                name STRING
            )
        """)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    def test_insert_data(self):
        self.duckdb_handler.insert_data('test_table', self.test_df)
        result = self.duckdb_handler.query('SELECT * FROM main.test_table')
        result = result.astype({'id': 'int64'})
        pd.testing.assert_frame_equal(result, self.test_df)

    def test_upsert_data(self):
        """
        Note: above methods have been executed already

        Input:
           id     name
        0   1    Alice
        1   2      Bob
        2   3  Charlie
        """
        # New data to upsert
        upsert_df = pd.DataFrame({
            'id': [2, 3, 4],
            'name': ['Bob', 'Cole', 'David']
        })

        # Expected result after upsert
        expected_df = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Cole', 'David']
        })
        self.duckdb_handler.upsert_data('test_table', upsert_df, ['id'])
        result = self.duckdb_handler.query('SELECT * FROM main.test_table')
        result = result.astype({'id': 'int64'})
        pd.testing.assert_frame_equal(result.sort_values('id').reset_index(drop=True), expected_df)

    def test_query(self):
        """
        Note: Above methods have been executed already.

        Input:
           id   name
        0   1  Alice
        1   2    Bob
        2   3   Cole
        3   4  David
        """
        result = self.duckdb_handler.query('SELECT * FROM main.test_table WHERE id = 1')
        result = result.astype({'id': 'int64'})
        expected_result = self.test_df[self.test_df['id'] == 1]
        pd.testing.assert_frame_equal(result, expected_result)

if __name__ == '__main__':
    unittest.main()
