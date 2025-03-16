import mysql.connector
import time

# Fungsi untuk membuat koneksi ke database
def connect_db():
    try:
        # Database Laptop A (Sumber)
        db_a = mysql.connector.connect(
            host="localhost",
            user="root",
            password="maria",
            database="jurnal",
            port=3307,
            charset="utf8mb4",
            collation="utf8mb4_general_ci"
        )

        # Database Komputer B (Tujuan)
        db_b_internasional = mysql.connector.connect(
            host="192.168.1.6",
            user="user_laptop",
            password="user_laptop",
            database="shard_internasional",
            port=3306,
            charset="utf8mb4",
            collation="utf8mb4_general_ci"
        )

        db_b_nasional = mysql.connector.connect(
            host="192.168.1.6",
            user="user_laptop",
            password="user_laptop",
            database="shard_nasional",
            port=3306,
            charset="utf8mb4",
            collation="utf8mb4_general_ci"
        )

        return db_a, db_b_internasional, db_b_nasional

    except mysql.connector.Error as err:
        print(f"Error koneksi ke database: {err}")
        return None, None, None


# Fungsi untuk membaca data baru dari Laptop A
def get_new_data(db_a):
    cursor_a = db_a.cursor(dictionary=True)
    cursor_a.execute("SELECT * FROM artikel WHERE id NOT IN (SELECT id FROM shard_log)")
    new_data = cursor_a.fetchall()
    cursor_a.close()
    return new_data


# Fungsi untuk menyimpan data ke Komputer B berdasarkan kategori
def insert_to_b(data, db_b_internasional, db_b_nasional):
    cursor_b_int = db_b_internasional.cursor()
    cursor_b_nas = db_b_nasional.cursor()

    for row in data:
        sql = "INSERT INTO artikel (id, judul, kategori, konten) VALUES (%s, %s, %s, %s)"
        values = (row['id'], row['judul'], row['kategori'], row['konten'])

        try:
            if row['kategori'].lower() == "internasional":
                cursor_b_int.execute(sql, values)
            elif row['kategori'].lower() == "nasional":
                cursor_b_nas.execute(sql, values)
        except mysql.connector.Error as err:
            print(f"Error saat memasukkan data ke shard: {err}")

    db_b_internasional.commit()
    db_b_nasional.commit()
    cursor_b_int.close()
    cursor_b_nas.close()


# Fungsi untuk mencatat data yang sudah dishard di Laptop A
def log_shard(data, db_a):
    cursor_a = db_a.cursor()
    for row in data:
        cursor_a.execute("INSERT INTO shard_log (id) VALUES (%s) ON DUPLICATE KEY UPDATE id=id", (row['id'],))
    db_a.commit()
    cursor_a.close()


# **Loop utama untuk menjalankan sharding**
if __name__ == "__main__":
    while True:
        db_a, db_b_internasional, db_b_nasional = connect_db()
        if not db_a or not db_b_internasional or not db_b_nasional:
            print("Gagal menghubungkan ke database, coba lagi dalam 10 detik...")
            time.sleep(10)
            continue

        new_data = get_new_data(db_a)

        if new_data:
            insert_to_b(new_data, db_b_internasional, db_b_nasional)
            log_shard(new_data, db_a)
            print(f"{len(new_data)} data baru dipindahkan ke Komputer B.")
        else:
            print("Tidak ada data baru.")

        db_a.close()
        db_b_internasional.close()
        db_b_nasional.close()

        time.sleep(10)  # Cek setiap 10 detik
