//Copyright (c) Chris Pietschmann 2015 (http://pietschsoft.com)
//Licensed under the GNU Library General Public License (LGPL)
//License can be found here: https://github.com/crpietschmann/SQLinq/blob/master/LICENSE

using System.Collections.Generic;

namespace SQLinq
{
    public interface ISqlDialect
    {
        object ConvertParameterValue(object value);
        string ParameterPrefix { get; }
        string ParseTableName(string tableName, string alias = null);
        string ParseColumnName(string columnName);
        void AssertSkip<T>(SQLinq<T> sqLinq);
        string ToQuery(SQLinqSelectResult selectResult);
        string IsNull(string col, object defaultValue, IDictionary<string, object> parameters);
    }
}
