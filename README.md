utf8-audit
==============
A Python script to check the UTF8 compliance of character strings in a PostgreSQL database.  If the string fails the UTF8 check the script attempts to decode it using ISO-8859-1, then re-encode it as UTF8.  A PostgreSQL-compliant UPDATE statement is generated and logged.  It may optionally be applied automatically, but that is not recommended.


Example usage of the utf8-audit db functions can be found in test-data/utf8-audit-test.sql, including a trigger function template for automatic conversion during inserts and updates.  The update technique *will* bloat your tables, so be sure to (auto)vacuum well and often!


### More Tests!

Please contribute further tests with known character sets and expected results.  The more thoroughly tested this module is the better.
# pg-utf8-audit
