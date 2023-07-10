import psycopg2


# удаление таблиц
def delete_all_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS telephone;
            DROP TABLE IF EXISTS client;
        """)


def create_structure_db(conn):
    # создать структуры таблиц
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(20) NOT NULL,
                last_name VARCHAR(20) NOT NULL,
                email VARCHAR(60) NOT NULL UNIQUE
            );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telephone(
                telephone_id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES client(client_id),
                phone_number VARCHAR(20) NOT NULL UNIQUE
            );
            """)
        conn.commit()  # выполнить команды


def add_new_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client(first_name, last_name, email) VALUES(%s, %s, %s) RETURNING client_id, first_name, last_name, email;
            """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        if phones:
            add_phone_exist_client(conn, client_id, phones)


def add_phone_exist_client(conn, client_id, phones):
    with conn.cursor() as cur:
        for phone in phones.split():
            cur.execute("""
                INSERT INTO telephone(client_id, phone_number) VALUES(%s, %s) RETURNING telephone_id, client_id, phone_number;
                    """, (client_id, phone))
            conn.commit()
            cur.execute("""
                SELECT first_name, last_name, email, phone_number
                FROM client AS CL LEFT JOIN telephone AS TL ON CL.client_id = TL.client_id
                GROUP BY CL.client_id, TL.phone_number
                ORDER BY CL.client_id, TL.phone_number;
                """)
            print(cur.fetchall())


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute("""
                UPDATE client
                SET first_name = %s
                WHERE client_id = %s;
                """, (first_name, client_id))
        if last_name:
            cur.execute("""
                UPDATE client
                SET last_name = %s
                WHERE client_id = %s;
                """, (last_name, client_id))
        if email:
            cur.execute("""
                UPDATE client
                SET email = %s
                WHERE client_id = %s;
                """, (email, client_id))
        cur.execute("""
            SELECT * FROM client
            ORDER BY client_id;
            """)
        print(cur.fetchall())
        if phones:
            cur.execute("""
                DELETE FROM telephone
                WHERE client_id = %s;
                """, (client_id,))
            conn.commit()
            add_phone_exist_client(conn, client_id, phones)


def delete_phone_exist_client(conn, client_id, phones=None):
    with conn.cursor() as cur:
        if phones:
            for phone in phones.split():
                cur.execute("""
                    DELETE FROM telephone
                    WHERE client_id = %s AND phone_number = %s;
                    """, (client_id, phone))
                conn.commit()
                cur.execute("""
                    SELECT first_name, last_name, email, phone_number
                    FROM client AS CL LEFT JOIN telephone AS TL ON CL.client_id = TL.client_id
                    GROUP BY CL.client_id, TL.phone_number
                    ORDER BY CL.client_id, TL.phone_number;
                    """)
                print(cur.fetchall())
        else:
            cur.execute("""
                DELETE FROM telephone
                WHERE client_id = %s;
                """, (client_id,))
            conn.commit()
            cur.execute("""
                SELECT first_name, last_name, email, phone_number
                FROM client AS CL LEFT JOIN telephone AS TL ON CL.client_id = TL.client_id
                GROUP BY CL.client_id, TL.phone_number
                ORDER BY CL.client_id, TL.phone_number;
                """)
            print(cur.fetchall())


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM telephone
            WHERE client_id = %s;
            """, (client_id,))
        cur.execute("""
            DELETE FROM client
            WHERE client_id = %s;
            """, (client_id,))
        cur.execute("""
            SELECT first_name, last_name, email, phone_number
            FROM client AS CL LEFT JOIN telephone AS TL ON CL.client_id = TL.client_id
            GROUP BY CL.client_id, TL.phone_number
            ORDER BY CL.client_id, TL.phone_number;
            """)
        print(cur.fetchall())


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        where_string_list = []
        num_rul = 0
        select_tel_flag = False
        if first_name:
            where_string_list.append("first_name LIKE '" + first_name + "'")
            num_rul += 1
        if last_name:
            where_string_list.append("last_name LIKE '" + last_name + "'")
            num_rul += 1
        if email:
            where_string_list.append("email LIKE '" + email + "'")
            num_rul += 1
        if phone:
            where_string_list.append("phone_number LIKE '" + phone + "'")
            select_tel_flag = True
            num_rul += 1
        if num_rul > 0:
            where_string = "WHERE "
            for i_rul in range(num_rul):
                where_string += where_string_list[i_rul]
                if i_rul < num_rul-1:
                    where_string += " AND "
        else:
            where_string = ""

        if select_tel_flag:
            cur.execute("""
                SELECT CL.client_id, first_name, last_name, email, phone_number
                FROM client AS CL LEFT JOIN telephone AS TL ON CL.client_id = TL.client_id """ +
                where_string +
                """ GROUP BY CL.client_id, first_name, last_name, email, phone_number
                ORDER BY CL.client_id, first_name, last_name, email, phone_number;
                """)
            try:
                client_id = cur.fetchone()[0]
                cur.execute("""
                    SELECT CL.client_id, first_name, last_name, email, phone_number
                    FROM client AS CL LEFT JOIN telephone AS TL ON CL.client_id = TL.client_id
                    WHERE CL.client_id = %s
                    GROUP BY CL.client_id, first_name, last_name, email, phone_number
                    ORDER BY CL.client_id, first_name, last_name, email, phone_number;
                    """,(client_id,))
            except:
                pass
        else:
            cur.execute("""
                SELECT first_name, last_name, email, phone_number
                FROM client AS CL LEFT JOIN telephone AS TL ON CL.client_id = TL.client_id """ +
                where_string +
                """ GROUP BY first_name, last_name, email, phone_number
                ORDER BY first_name, last_name, email, phone_number;
                """)
        print(cur.fetchall())


if __name__ == '__main__':
    with psycopg2.connect(database="clients_db", user="postgres", password="123456789") as conn:
        delete_all_tables(conn)
        create_structure_db(conn)
        add_new_client(conn, "Иван", "Иванов", "IvanIvanov@gmail.com", "+7(999)999-99-99")
        add_new_client(conn, "Петр", "Петров", "PetrPetrov@gmail.com")
        add_new_client(conn, "Николай", "Николаев", "NikolayNikolayev@gmail.com", "+7(888)888-88-88 +7(777)777-77-77")
        add_phone_exist_client(conn, 1, "+7(666)666-66-66")
        add_phone_exist_client(conn, 2, "+7(555)555-55-55 +7(444)444-44-44")
        add_phone_exist_client(conn, 3, "+7(333)333-33-33")
        change_client(conn, 1, "Ваня")
        change_client(conn, 1, None, "Ваняев")
        change_client(conn, 1, None, None, "VanyaVanyaev@gmail.com")
        change_client(conn, 2, "Петя", "Петраков")
        change_client(conn, 2, "Петя", None, "PetyaPetrakov@gmail.com")
        change_client(conn, 2, None, "Петраков", "PetyaPetrakov@gmail.com")
        change_client(conn, 3, "Коля", "Коляев", "KolyaKolyaev@gmail.com")
        change_client(conn, 1, "Иван", "Иванов", "IvanIvanov@gmail.com", "+7(iii)iii-ii-ii")
        change_client(conn, 2, "Петр", "Петров", "PetrPetrov@gmail.com")
        change_client(conn, 3, "Николай", "Николаев", "NikolayNikolayev@gmail.com", "+7(nnn)nnn-nn-nn +7(mmm)mmm-mm-mm")
        change_client(conn, 1, "Иван", "Иванов", "IvanIvanov@gmail.com", "+7(999)999-99-99")
        change_client(conn, 2, "Петр", "Петров", "PetrPetrov@gmail.com", "+7(666)666-66-66 +7(555)555-55-55")
        change_client(conn, 3, "Николай", "Николаев", "NikolayNikolayev@gmail.com", "+7(888)888-88-88 +7(777)777-77-77")
        delete_phone_exist_client(conn, 1, "+7(999)999-99-99")
        delete_phone_exist_client(conn, 2, "+7(555)555-55-55")
        delete_phone_exist_client(conn, 3, "+7(888)888-88-88 +7(777)777-77-77")
        change_client(conn, 1, "Иван", "Иванов", "IvanIvanov@gmail.com", "+7(999)999-99-99")
        change_client(conn, 2, "Петр", "Петров", "PetrPetrov@gmail.com", "+7(666)666-66-66 +7(555)555-55-55")
        change_client(conn, 3, "Николай", "Николаев", "NikolayNikolayev@gmail.com", "+7(888)888-88-88 +7(777)777-77-77")
        delete_phone_exist_client(conn, 1)
        delete_phone_exist_client(conn, 3)
        change_client(conn, 1, "Иван", "Иванов", "IvanIvanov@gmail.com", "+7(999)999-99-99")
        change_client(conn, 2, "Петр", "Петров", "PetrPetrov@gmail.com", "+7(666)666-66-66 +7(555)555-55-55")
        change_client(conn, 3, "Николай", "Николаев", "NikolayNikolayev@gmail.com", "+7(888)888-88-88 +7(777)777-77-77")
        delete_client(conn, 1)
        delete_client(conn, 3)
        delete_client(conn, 2)
        add_new_client(conn, "Иван", "Иванов", "IvanIvanov@gmail.com", "+7(999)999-99-99")
        add_new_client(conn, "Петр", "Петров", "PetrPetrov@gmail.com")
        add_new_client(conn, "Николай", "Николаев", "NikolayNikolayev@gmail.com", "+7(888)888-88-88 +7(777)777-77-77")
        find_client(conn, "Петр")
        find_client(conn, "Петр", "Козлов")
        find_client(conn, None, "Петров", None, None)
        find_client(conn, None, "Николаев", None, "+7(777)777-77-77")
        find_client(conn, None, None, None, "+7(777)777-77-77")
        find_client(conn, None, "Николаев", None, "78788686878978787")
        find_client(conn, None, "Петров", None, "78788686878978787")
        find_client(conn, None, "Сидоров", None, "78788686878978787")






