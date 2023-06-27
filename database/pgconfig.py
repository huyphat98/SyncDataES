import psycopg2

def create_connection():
    """
    Tạo kết nối đến cơ sở dữ liệu PostgreSQL.

    Input:
        Không có input.

    Output:
        conn: Đối tượng kết nối đến cơ sở dữ liệu PostgreSQL.
    """
    conn = psycopg2.connect(
        host="localhost",
        database="voiads",
        user="postgres",
        password="1111"
    )
    return conn