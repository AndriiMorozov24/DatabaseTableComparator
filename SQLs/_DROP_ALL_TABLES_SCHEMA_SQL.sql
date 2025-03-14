USE AdventureWorksLT2022;

DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];' + CHAR(13)
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'STIR';

PRINT @sql;  -- Optional: view the generated DROP statements

EXEC sp_executesql @sql;
