from fastapi import FastAPI, HTTPException
import psycopg2

app = FastAPI()

# Connection parameters
db_name = 'postgres'
db_user = 'postgres'
db_pass = 'postgres'
db_host = 'localhost'
db_port = '5432'

# Define a function to fetch data from the database
def fetch_data():
    conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass, port=db_port)
    cur = conn.cursor()
    cur.execute("SELECT * FROM student")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows

# Define a route to serve the data
@app.get("/data/")
def read_data():
    # Fetch data from the database
    data = fetch_data()

    # Return the fetched data as a response
    return {"data": data}

def insert_data(name: str, city: str):
    try:
        conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Execute an SQL INSERT query to insert data into the "student" table
        cur.execute("INSERT INTO student (name, city) VALUES (%s, %s)", (name, city))

        # Commit the transaction
        conn.commit()

        # Close the cursor
        cur.close()

        # Close the connection
        conn.close()

        return {"message": "Data inserted successfully"}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

def update_data(id: int, name: str, city: str):
    try:
        conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Execute an SQL UPDATE query to update data in the "student" table
        cur.execute("UPDATE student SET name = %s, city = %s WHERE id = %s", (name, city, id))

        # Commit the transaction
        conn.commit()

        # Close the cursor
        cur.close()

        # Close the connection
        conn.close()

        return {"message": "Data updated successfully"}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

def delete_data(id: int):
    try:
        conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Execute an SQL DELETE query to delete data from the "student" table
        cur.execute("DELETE FROM student WHERE id = %s", (id,))

        # Check if any row was affected by the DELETE operation
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Record not found")

        # Commit the transaction
        conn.commit()

        # Close the cursor
        cur.close()

        # Close the connection
        conn.close()

        return {"message": "Data deleted successfully"}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

def search_data(city: str):
    try:
        conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Execute an SQL DELETE query to delete data from the "student" table
        cur.execute("select * FROM student WHERE city = %s", (city))

        rows = cur.fetchall()

        # Close the cursor
        cur.close()

        # Close the connection
        conn.close()

        if rows:
            return rows
        else:
            return {"message": "No data found for the given criteria"}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error{e}")

# Define a route to insert data into the database
@app.post("/insert/")
def insert_student_data(name: str, city: str):
    return insert_data(name, city)

@app.put("/update/")
def update_student_data(id: int, name: str, city: str):
    return update_data(id, name, city)

@app.delete("/delete/")
def delete_student_data(id: int):
    return delete_data(id)

@app.get("/search/")
def search_student_data(city: str):
    return search_data(city)