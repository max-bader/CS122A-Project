import mysql.connector
import sys
import csv
import os

def get_db_connection():
    """Connects to MySQL database"""
    return mysql.connector.connect(
        host="localhost",
        user="test",
        password="password"
        #database="agent_platform"
    )

def import_data(folder_name):
    """Question 1: Import data from CSV files into mySQL database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        tables = [
            "ModelConfigurations", "ModelServices", "DataStorage", "LLMService",
            "InternetService", "Configuration", "CustomizedModel", "BaseModel",
            "AgentClient", "AgentCreator", "Users"
        ]
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        # Create all tables needed
        ddl_statements = [
            """CREATE TABLE Users (
                uid INT,
                email TEXT NOT NULL,
                username TEXT NOT NULL,
                PRIMARY KEY (uid)
            )""",
            """CREATE TABLE AgentCreator (
                uid INT,
                bio TEXT,
                payout TEXT,
                PRIMARY KEY (uid),
                FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE AgentClient (
                uid INT,
                interests TEXT NOT NULL,
                cardholder TEXT NOT NULL,
                expire DATE NOT NULL,
                cardno INT NOT NULL,
                cvv INT NOT NULL,
                zip INT NOT NULL,
                PRIMARY KEY (uid),
                FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE BaseModel (
                bmid INT,
                creator_uid INT NOT NULL,
                description TEXT NOT NULL,
                PRIMARY KEY (bmid),
                FOREIGN KEY (creator_uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE CustomizedModel (
                bmid INT,
                mid INT NOT NULL,
                PRIMARY KEY (bmid, mid),
                FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
            )""",
            """CREATE TABLE Configuration (
                cid INT,
                client_uid INT NOT NULL,
                content TEXT NOT NULL,
                labels TEXT NOT NULL,
                PRIMARY KEY (cid),
                FOREIGN KEY (client_uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE InternetService (
                sid INT,
                provider TEXT NOT NULL,
                endpoints TEXT NOT NULL,
                PRIMARY KEY (sid)
            )""",
            """CREATE TABLE LLMService (
                sid INT,
                domain TEXT,
                PRIMARY KEY (sid),
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE DataStorage (
                sid INT,
                type TEXT,
                PRIMARY KEY (sid),
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE ModelServices (
                bmid INT NOT NULL,
                sid INT NOT NULL,
                version INT NOT NULL,
                PRIMARY KEY (bmid, sid),
                FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE ModelConfigurations (
                bmid INT NOT NULL,
                mid INT NOT NULL,
                cid INT NOT NULL,
                duration INT NOT NULL,
                PRIMARY KEY (bmid, mid, cid),
                FOREIGN KEY (bmid, mid) REFERENCES CustomizedModel(bmid, mid) ON DELETE CASCADE,
                FOREIGN KEY (cid) REFERENCES Configuration(cid) ON DELETE CASCADE
            )"""
        ]
        
        for ddl in ddl_statements:
            cursor.execute(ddl)
        
        # Import all CSV files for testing
        csv_files = [
            ("User.csv", "Users", 3),
            ("AgentCreator.csv", "AgentCreator", 3),
            ("AgentClient.csv", "AgentClient", 7),
            ("BaseModel.csv", "BaseModel", 3),
            ("CustomizedModel.csv", "CustomizedModel", 2),
            ("Configuration.csv", "Configuration", 4),
            ("InternetService.csv", "InternetService", 3),
            ("LLMService.csv", "LLMService", 2),
            ("DataStorage.csv", "DataStorage", 2),
            ("ModelServices.csv", "ModelServices", 3),
            ("ModelConfigurations.csv", "ModelConfigurations", 4)
        ]
        
        for csv_file, table_name, num_cols in csv_files:
            file_path = os.path.join(folder_name, csv_file)
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    reader = csv.reader(f)
                    next(reader)
                    placeholders = ','.join(['%s'] * num_cols)
                    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    for row in reader:
                        row = [None if x == 'NULL' else x for x in row]
                        cursor.execute(insert_query, row)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Success")
        return True
    except Exception as e:
        print("Fail")
        return False

def insertAgentClient(uid, username, email, card_number, card_holder, expiration_date, cvv, zip_code, interests):
    """Question 2: Inserts a new agent client"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
    
        cursor.execute(
            "INSERT INTO Users (uid, username, email) VALUES (%s, %s, %s)",
            (uid, username, email)
        )
        
        cursor.execute(
            """INSERT INTO AgentClient (uid, interests, cardholder, expire, cardno, cvv, zip) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (uid, interests, card_holder, expiration_date, card_number, cvv, zip_code)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Success")
        return True
    except Exception as e:
        print("Fail")
        return False

def addCustomizedModel(mid, bmid):
    """Question 3: Adds the customized model"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO CustomizedModel (bmid, mid) VALUES (%s, %s)",
            (bmid, mid)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Success")
        return True
    except Exception as e:
        print("Fail")
        return False

def deleteBaseModel(bmid):
    """Question 4: Deletes the base model"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM BaseModel WHERE bmid = %s", (bmid,))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Success")
        return True
    except Exception as e:
        print("Fail")
        return False

def listInternetService(bmid):
    """Question 5: Lists the internet services needed for the base model"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT i.sid, i.endpoints, i.provider
            FROM InternetService i
            JOIN ModelServices ms ON i.sid = ms.sid
            WHERE ms.bmid = %s
            ORDER BY i.provider ASC
        """
        
        cursor.execute(query, (bmid,))
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        return False

def countCustomizedModel(*bmids):
    """Question 6: Counts the customized models for the given base model IDs"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        bmid_list = list(bmids)
        
        placeholders = ','.join(['%s'] * len(bmid_list))
        query = f"""
            SELECT bm.bmid, bm.description, COUNT(cm.mid) as customizedModelCount
            FROM BaseModel bm
            LEFT JOIN CustomizedModel cm ON bm.bmid = cm.bmid
            WHERE bm.bmid IN ({placeholders})
            GROUP BY bm.bmid, bm.description
            ORDER BY bm.bmid ASC
        """
        
        cursor.execute(query, bmid_list)
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        return False

def topNDurationConfig(uid, n):
    """Question 7: Finds the top N longest duration configurations for a client"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT c.client_uid, c.cid, c.labels, c.content, mc.duration
            FROM Configuration c
            JOIN ModelConfigurations mc ON c.cid = mc.cid
            WHERE c.client_uid = %s
            ORDER BY mc.duration DESC
            LIMIT %s
        """
        
        cursor.execute(query, (uid, n))
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        return False

def listBaseModelKeyWord(keyword):
    """Question 8: Lists base models using LLM services with certain keyword in domain"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT DISTINCT bm.bmid, i.sid, i.provider, l.domain
            FROM BaseModel bm
            JOIN ModelServices ms ON bm.bmid = ms.bmid
            JOIN InternetService i ON ms.sid = i.sid
            JOIN LLMService l ON i.sid = l.sid
            WHERE l.domain LIKE %s
            ORDER BY bm.bmid ASC
            LIMIT 5
        """
        
        cursor.execute(query, (f"%{keyword}%",))
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]},{row[3]}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        return False

def printNL2SQLresult():
    """Question 9: Prints the NL2SQL experiment results from CSV file"""
    try:
        csv_file = "nl2sql_results.csv"
        
        if not os.path.exists(csv_file):
            print("Error: nl2sql_results.csv not found")
            return False
        
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                print(','.join(str(x) for x in row))
        
        return True
    except Exception as e:
        return False

def main():
    """Main function that handles command-line arguments and runs the necessary code"""
    if len(sys.argv) < 2:
        print("Error: No function specified")
        return
    
    function_name = sys.argv[1]
    
    if function_name == "import":
        if len(sys.argv) < 3:
            print("Error: Missing folder name")
            return
        import_data(sys.argv[2])
    
    elif function_name == "insertAgentClient":
        if len(sys.argv) < 11:
            print("Error: Missing parameters")
            return
        insertAgentClient(
            int(sys.argv[2]),
            sys.argv[3],
            sys.argv[4],        
            int(sys.argv[5]), 
            sys.argv[6],
            sys.argv[7],
            int(sys.argv[8]),
            int(sys.argv[9]),
            sys.argv[10]
        )
    
    elif function_name == "addCustomizedModel":
        if len(sys.argv) < 4:
            print("Error: Missing parameters")
            return
        addCustomizedModel(int(sys.argv[2]), int(sys.argv[3]))
    
    elif function_name == "deleteBaseModel":
        if len(sys.argv) < 3:
            print("Error: Missing parameters")
            return
        deleteBaseModel(int(sys.argv[2]))
    
    elif function_name == "listInternetService":
        if len(sys.argv) < 3:
            print("Error: Missing parameters")
            return
        listInternetService(int(sys.argv[2]))
    
    elif function_name == "countCustomizedModel":
        if len(sys.argv) < 3:
            print("Error: Missing parameters")
            return
        bmids = [int(x) for x in sys.argv[2:]]
        countCustomizedModel(*bmids)
    
    elif function_name == "topNDurationConfig":
        if len(sys.argv) < 4:
            print("Error: Missing parameters")
            return
        topNDurationConfig(int(sys.argv[2]), int(sys.argv[3]))
    
    elif function_name == "listBaseModelKeyWord":
        if len(sys.argv) < 3:
            print("Error: Missing parameters")
            return
        listBaseModelKeyWord(sys.argv[2])
    
    elif function_name == "printNL2SQLresult":
        printNL2SQLresult()
    
    else:
        print(f"Error: Unknown function '{function_name}'")

if __name__ == "__main__":
    main()
