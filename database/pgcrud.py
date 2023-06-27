from database.pgconfig import create_connection
import json

def create_data(table_name, **kwargs):
    """
    Thêm một bản ghi mới vào bảng được chỉ định.

    Input:
        table_name: Tên bảng cần thêm bản ghi.
        **kwargs: Các trường dữ liệu và giá trị tương ứng của bản ghi mới.

    Output:
        Không có output.
    """
    conn = create_connection()
    cursor = conn.cursor()
    columns = ', '.join(kwargs.keys())
    values = ', '.join(['%s'] * len(kwargs))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
    data = tuple(kwargs.values())
    cursor.execute(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()
    return {
        'code':200,
        'status': 'create record successfuly.'
    }

def read_data(table_name):
    """
    Đọc tất cả các bản ghi từ bảng được chỉ định.

    Input:
        table_name: Tên bảng cần đọc dữ liệu.

    Output:
        rows: Danh sách các bản ghi từ bảng.
    """
    conn = create_connection()
    cursor = conn.cursor()
    select_query = f"SELECT * FROM {table_name};"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    # Lấy thông tin về các cột từ cursor.description
    columns = [desc[0] for desc in cursor.description]

    # Tạo danh sách đối tượng JSON với key và value tương ứng
    json_data = []
    for row in rows:
        json_row = {}
        for i, column in enumerate(columns):
            json_row[column] = row[i]
        json_data.append(json_row)

    # Chuyển đổi danh sách thành chuỗi JSON
    json_output = json.dumps(json_data)
    cursor.close()
    conn.close()
    return {
        'code':200,
        'status': 'read data successfuly.',
        'data_table':  {
            'table': table_name,
            'data': json_output
        }
    }

def update_data(table_name, record_id, **kwargs):
    """
    Cập nhật bản ghi trong bảng được chỉ định với các trường dữ liệu mới.

    Input:
        table_name: Tên bảng cần cập nhật bản ghi.
        record_id: ID của bản ghi cần cập nhật.
        **kwargs: Các trường dữ liệu và giá trị tương ứng để cập nhật.

    Output:
        Không có output.
    """
    conn = create_connection()
    cursor = conn.cursor()
    set_values = ', '.join([f"{key} = %s" for key in kwargs.keys()])
    update_query = f"UPDATE {table_name} SET {set_values} WHERE id = %s"
    data = tuple(list(kwargs.values()) + [record_id])
    cursor.execute(update_query, data)
    conn.commit()
    cursor.close()
    conn.close()
    return {
        'code':200,
        'status': 'update record successfuly.'
    }

def delete_data(table_name, record_id):
    """
    Xóa bản ghi trong bảng được chỉ định dựa trên ID.

    Input:
        table_name: Tên bảng cần xóa bản ghi.
        record_id: ID của bản ghi cần xóa.

    Output:
        Không có output.
    """
    conn = create_connection()
    cursor = conn.cursor()
    delete_query = f"DELETE FROM {table_name} WHERE id = %s"
    data = (record_id,)
    cursor.execute(delete_query, data)
    conn.commit()
    cursor.close()
    conn.close()
    return {
        'code':200,
        'status': 'delete record successfuly.'
    }
