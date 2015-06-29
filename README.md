utf8-audit
==============
A Python script to check the UTF8 compliance of character strings in a PostgreSQL database.  If the string fails the UTF8 check the script attempts to decode it using Latin1, then re-encode it as UTF8.  A PostgreSQL-compliant UPDATE statement is generated and logged.  It may optionally be applied automatically, but that is not recommended.

### Dependencies
The script depends on the db functions in the SQL directory of https://github.com/aweber/pg-utf8-transcoder.  Download that project and follow the README to install the functions.
