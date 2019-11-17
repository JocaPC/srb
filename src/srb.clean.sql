use Test;
go
--------------------------------------------------------------------------------
--	SQL Server & Azure SQL Managed Instance - SeRvice Broker library
--	Author: Jovan Popovic
--------------------------------------------------------------------------------
DECLARE @name VARCHAR(128)
DECLARE @SQL VARCHAR(MAX)


/* Drop all synonyms */

SELECT @name = (SELECT TOP 1 [name] FROM sys.synonyms WHERE SCHEMA_NAME(schema_id) = 'srb' ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP SYNONYM [srb].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Synonym: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.synonyms WHERE SCHEMA_NAME(schema_id) = 'srb'  AND [name] > @name ORDER BY [name])
END

SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'P' AND SCHEMA_NAME(schema_id) = 'srb'  ORDER BY [name])

WHILE @name is not null
BEGIN
    SELECT @SQL = 'DROP PROCEDURE [srb].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Procedure: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'P' AND SCHEMA_NAME(schema_id) = 'srb'  AND [name] > @name ORDER BY [name])
END

/* Drop all views */

SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'V' AND SCHEMA_NAME(schema_id) = 'srb'  ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP VIEW [srb].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped View: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'V' AND SCHEMA_NAME(schema_id) = 'srb'  AND [name] > @name ORDER BY [name])
END

/* Drop all functions */
SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND SCHEMA_NAME(schema_id) = 'srb'  ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP FUNCTION [srb].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Function: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND SCHEMA_NAME(schema_id) = 'srb'  AND [name] > @name ORDER BY [name])
END

/* Drop all tables */

SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'U'  AND SCHEMA_NAME(schema_id) = 'srb' ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP TABLE [srb].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Table: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'U'  AND SCHEMA_NAME(schema_id) = 'srb'  AND [name] > @name ORDER BY [name])
END
GO

GO
DROP TYPE IF EXISTS srb.Messages;
GO
DROP TYPE IF EXISTS srb.Strings;
GO
DROP TYPE IF EXISTS srb.Integers;
GO
BEGIN TRY
DROP CONTRACT [DefaultContract]
END TRY BEGIN CATCH END CATCH
GO

BEGIN TRY
DROP CONTRACT [DefaultSenderContract]
END TRY BEGIN CATCH END CATCH
GO

DROP SCHEMA IF EXISTS srb;

