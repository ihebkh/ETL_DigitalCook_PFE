import unittest
import psycopg2
from unittest.mock import patch, MagicMock
def get_postgres_cursor():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="iheb",
        password="201JmT1896@",
        host="monserveur-postgres.postgres.database.azure.com",
        port="5432"
    ), None 

class TestPostgresConnection(unittest.TestCase):

    @patch('psycopg2.connect')
    def test_get_postgres_cursor(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        conn, _ = get_postgres_cursor()

        mock_connect.assert_called_with(
            dbname="DW_DigitalCook",
            user="iheb",
            password="201JmT1896@",
            host="monserveur-postgres.postgres.database.azure.com",
            port="5432"
        )
        self.assertEqual(conn, mock_conn)

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
