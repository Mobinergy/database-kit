/// All supported values for a SQL `DataPredicate`
public enum DataPredicateValue {
    /// No value.
    case none

    /// One or more placeholders.
    case placeholders(count: Int)

    /// A single placeholder.
    public static let placeholder = DataPredicateValue.placeholders(count: 1)

    /// Compare to another column in the database.
    case column(DataColumn)

    /// Serializes a complete sub-query as this predicate's value.
    case subquery(DataQuery)

    /// Custom string that will be interpolated into the SQL query.
    /// note: Be careful about SQL injection when using this.
    case custom(String)
}
