extension SQLSerializer {
    /// See SQLSerializer.serialize(schema:)
    public func serialize(schema query: SchemaQuery) -> String {
        var statement: [String] = []
        let table = makeEscapedString(from: query.table)

        switch query.statement {
        case .create:
            statement.append("CREATE TABLE")
            statement.append(table)

            let columns = query.addColumns.map { serialize(column: $0) }
                + query.addForeignKeys.map { serialize(foreignKey: $0) }
            statement.append("(" + columns.joined(separator: ", ") + ")")
        case .alter:
            statement.append("ALTER TABLE")
            statement.append(table)

            let adds = query.addColumns.map { "ADD " + serialize(column: $0) }
            if adds.count > 0 {
                statement.append(adds.joined(separator: ", "))
            }

            let deletes = query.deleteColumns.map { "DROP " + makeEscapedString(from: $0) }
            if deletes.count > 0 {
                statement.append(deletes.joined(separator: ", "))
            }

            let deleteFKs = query.deleteForeignKeys.map { "DROP FOREIGN KEY " + makeEscapedString(from: $0) }
            if deleteFKs.count > 0 {
                statement.append(deleteFKs.joined(separator: ", "))
            }
        case .drop:
            statement.append("DROP TABLE")
            statement.append(table)
        }

        return statement.joined(separator: " ")
    }

    /// See SQLSerializer.serialize(column:)
    public func serialize(column: SchemaColumn) -> String {
        var sql: [String] = []

        let name = makeEscapedString(from: column.name)
        sql.append(name)
        sql.append(column.dataType)
        return sql.joined(separator: " ")
    }

    /// See SQLSerializer.serialize(foreignKey:)
    public func serialize(foreignKey: SchemaForeignKey) -> String {
        // FOREIGN KEY(trackartist) REFERENCES artist(artistid)
        var sql: [String] = []

        sql.append("FOREIGN KEY")

        if let table = foreignKey.local.table {
            sql.append(makeEscapedString(from: table))
        }
        sql.append("(" + makeEscapedString(from: foreignKey.local.name) + ")")

        sql.append("REFERENCES")

        if let table = foreignKey.foreign.table {
            sql.append(makeEscapedString(from: table))
        }
        sql.append("(" + makeEscapedString(from: foreignKey.foreign.name) + ")")

        if let onUpdate = foreignKey.onUpdate {
            sql.append("ON UPDATE")
            sql.append(serialize(foreignKeyAction: onUpdate))
        }

        if let onDelete = foreignKey.onDelete {
            sql.append("ON DELETE")
            sql.append(serialize(foreignKeyAction: onDelete))
        }

        return sql.joined(separator: " ")
    }

    /// See SQLSerializer.serialize(foreignKeyAction:)
    public func serialize(foreignKeyAction: SchemaForeignKeyAction) -> String {
        switch foreignKeyAction {
        case .noAction: return "NO ACTION"
        case .restrict: return "RESTRICT"
        case .setNull: return "SET NULL"
        case .setDefault: return "SET DEFAULT"
        case .cascade: return "CASCADE"
        }
    }
}
