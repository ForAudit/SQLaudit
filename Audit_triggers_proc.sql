USE [Payments]
GO

/****** Object:  StoredProcedure [audit].[audit_triggers]    Script Date: 25.03.2020 10:38:35 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [audit].[audit_triggers]
    @schema_name sysname = 'dbo',
	@action varchar(16) = 'create'
AS
BEGIN
	DECLARE @_schema_name sysname;
	DECLARE @_table_name sysname;
    DECLARE @_tsql nvarchar(2048);

    -- Список таблиц
    DECLARE var_cursor CURSOR FOR    
        SELECT s.name as _schema_name, t.name as _table_name
        FROM sys.tables t join sys.schemas s on t.schema_id = s.schema_id
        WHERE s.name = @schema_name
		AND  t.lob_data_space_id = 0;
    OPEN var_cursor;
    FETCH NEXT FROM var_cursor 
        INTO @_schema_name,  @_table_name;

    WHILE (@@fetch_status = 0)
    BEGIN
		-- формирование команды удаления существующего триггера
		SET @_tsql = '';
		SET @_tsql += 'IF OBJECT_ID(''['+ @_schema_name + '].[trg_' + @_table_name + ']'') IS NOT NULL ' + CHAR(13) 
		SET @_tsql += '    DROP TRIGGER ['+ @_schema_name + '].[trg_' + @_table_name + '];';

		IF (@action = 'create') OR (@action = 'drop')
		BEGIN
			EXEC(@_tsql);
        END

		-- Создание триггера
		IF (@action = 'create')
			BEGIN
				SET @_tsql = '';
				SET @_tsql += 'CREATE TRIGGER ['+ @_schema_name + '].[trg_' + @_table_name + '] '
				SET @_tsql += 'ON ['+ @_schema_name + '].[' + @_table_name + '] ' + CHAR(13)
				SET @_tsql += 'FOR INSERT, UPDATE, DELETE AS ' + CHAR(13)
				SET @_tsql += 'BEGIN ' + CHAR(13)

				SET @_tsql += 'IF EXISTS (select * from inserted) AND NOT EXISTS (select * from deleted)' + CHAR(13)
				SET @_tsql += 'BEGIN' + CHAR(13)
				SET @_tsql += '    INSERT [audit].[log_table_changes] ([_type], [schema_name], [object_name], [xml_recset])' + CHAR(13)
				SET @_tsql += '    SELECT ''INSERT'', ''[' +  @_schema_name + ']'', ''[' + @_table_name + ']'',' 
				SET @_tsql += ' (SELECT * FROM inserted as Record for xml auto, elements , root(''RecordSet''), type)' + CHAR(13)
				SET @_tsql += '    RETURN;' + CHAR(13)
				SET @_tsql += 'END' + CHAR(13) + CHAR(13)

				SET @_tsql += 'IF EXISTS (select * from deleted) AND NOT EXISTS (select * from inserted)' + CHAR(13)
				SET @_tsql += 'BEGIN' + CHAR(13)
				SET @_tsql += '    INSERT [audit].[log_table_changes] ([_type], [schema_name], [object_name], [xml_recset])' + CHAR(13)
				SET @_tsql += '    SELECT ''DELETE'', ''[' +  @_schema_name + ']'', ''[' + @_table_name + ']'',' 
				SET @_tsql += ' (SELECT * FROM deleted as Record for xml auto, elements , root(''RecordSet''), type)' + CHAR(13)
				SET @_tsql += '    RETURN;' + CHAR(13)
				SET @_tsql += 'END' + CHAR(13) + CHAR(13)

				SET @_tsql += 'IF EXISTS (select * from inserted) AND EXISTS (select * from deleted)' + CHAR(13)
				SET @_tsql += 'BEGIN' + CHAR(13)
				SET @_tsql += '    INSERT [audit].[log_table_changes] ([_type], [schema_name], [object_name], [xml_recset])' + CHAR(13)
				SET @_tsql += '    SELECT ''UPDATE'', ''[' +  @_schema_name + ']'', ''[' + @_table_name + ']'',' 
				SET @_tsql += ' (SELECT * FROM deleted as Record for xml auto, elements , root(''RecordSet''), type)' + CHAR(13)
				SET @_tsql += '    RETURN;' + CHAR(13)
				SET @_tsql += 'END' + CHAR(13) + CHAR(13)

				SET @_tsql += 'END; ' + CHAR(13)
			END
			BEGIN
				EXEC(@_tsql);
			END

        FETCH NEXT FROM var_cursor 
        INTO @_schema_name,  @_table_name;
	END
    CLOSE var_cursor;
    DEALLOCATE var_cursor;
END;
GO


