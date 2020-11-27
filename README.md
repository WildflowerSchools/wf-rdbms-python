# wf_rdbms

Python tools for defining and interacting with simple relational databases

## Tasks
* Split repo into wf_rdbms and wf_core_data
* Redesign wf_rdbms around underlying SQL implementation while preserving Pandas implementation
* Implement wf_rdbms in Postgres
* Implement wf_rdbms in SQLite
* Add method(s) to extract the current info for each student
* Add method(s) to suggest and review student dupes
* Fix inheritance structure so `Database` init can happen at parent class level
* Make database structure an OrderedDict so user can specify order of data tables (e.g., for saving to Google sheets)
* Add method for writing database to Google Sheets
* Add method for writing database to local file(s)
* Add tables for schools, classrooms, teachers
* Generalize student ID generation to other objects (teachers, schools, hubs, etc.)
* Use `__getitem__` so that user can pull individual data tables without referencing internals
* Define generic `WildflowerDatabase` and make `WildflowerDatabasePandas` a subclass
* Move methods for pulling and adding student records to parent `WildflowerDatabase` class (so they can be used by other database implementations)
