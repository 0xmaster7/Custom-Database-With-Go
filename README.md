Go Database Project
===================

A Go-based relational database implementation, created by referencing the **2nd Edition of** _**Build Your Own Database From Scratch**_.

This project implements the foundational layers of a SQL database: a strict recursive descent parser and a persistent, on-disk B+Tree storage engine.

🚧 Current Status
-----------------

**This project is a proof-of-concept, not a complete database.**

The primary focus was building:

*   the **parser** (the "front-end" that understands SQL)
    
*   and the **storage engine** (the "back-end" that saves data)
    

The "middle" layer—the query execution engine that connects the parser to the storage—is mostly a placeholder.

⚙️ How to Run
-------------

### Prerequisites

You must have the [Go programming language](https://go.dev/doc/install) installed.

### Run Tests

Navigate to the project directory and run:

`   go run main.go   `

The program will execute a series of built-in tests that verify the **B+Tree functionality** and the **SQL parser**.

It will create and then delete a sql\_test.db file in the same directory as part of its self-cleaning test.

✨ Features & Implemented Concepts
---------------------------------

🔹 **Recursive Descent SQL Parser**A hand-written, top-down parser that translates SQL strings into internal Go structs.

🔹 **Persistent B+Tree Storage**The core storage engine is a **B+Tree**, which stores all data in **4KB pages** on disk — ideal for minimizing disk I/O.

🔹 **mmap Page Management**Uses **Memory-Mapped I/O (mmap)** to map the database file directly into memory, allowing the OS to handle all page caching and disk reads efficiently.

🔹 **Freelist for Space Management**A linked list of deleted pages is maintained to allow for efficient reuse of disk space.

🔹 **CREATE TABLE Execution**This is the one SQL command that is fully implemented.The parser understands the command, and the table's schema (column names/types) is correctly saved to the database file.

🔹 **Snapshot Isolation (Basic)**A basic transaction model that allows reads to occur on a "snapshot" without being blocked by concurrent writes.

⚠️ Limitations & Missing Features
---------------------------------

*   **SELECT Execution is a Placeholder:**The parser _understands_ SELECT queries, but the execution engine **does not** scan the database. It returns a hardcoded "john" record, regardless of the query.
    
*   **No INSERT, UPDATE, or DELETE:**These DML commands are **not implemented**. The parser does not recognize them, and there is no execution logic for them.
    
*   **Dummy FILTER Logic:**The FILTER clause is parsed, but the execution logic to actually filter data is not implemented.
    

🚀 Future Scope
---------------

This project's foundation is solid, but it requires a **query execution engine** to become a functional database.Future work would include:

*   \[ \] **Full SELECT Query Execution**Replace the dummy dbScan and Deref functions with real B+Tree scanning logic.
    
*   \[ \] **Implement DML Commands**
    
    *   \[ \] INSERT INTO ... VALUES (...)
        
    *   \[ \] DELETE FROM ...
        
    *   \[ \] UPDATE ... SET ...
        
*   \[ \] **Build a Query Planner**A simple optimizer to decide _how_ to execute a query (e.g., index scan vs. full table scan).
    
*   \[ \] **Secondary Index Support**Implement logic for creating, updating, and using secondary indexes for faster lookups on non-primary-key columns.
    
*   \[ \] **Richer SQL Grammar**Expand the parser to support common clauses like:
    
    *   \[ \] WHERE (as an alias for FILTER)
        
    *   \[ \] AND / OR logical operators
        
    *   \[ \] GROUP BY
        
*   \[ \] **Robust Error Handling**Replace all panic() calls in the parser with proper error returns, so invalid SQL returns a "syntax error" instead of crashing.
    

🧠 Supported SQL Grammar
------------------------

The current strict parser **only** recognizes the following tokens.Any other keyword (like WHERE) or invalid syntax will cause the program to panic.

### Keywords

SELECT CREATE TABLE FROM AS INDEX BY AND FILTER LIMIT PRIMARY KEY

### Types

string text int integer int64

### Symbols

\* , ( ) ' (for strings)