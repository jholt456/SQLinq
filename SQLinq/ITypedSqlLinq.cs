using System.Collections.Generic;
using System.Linq.Expressions;

namespace SQLinq
{
    public interface ITypedSqlLinq : ISQLinq
    {
        List<Expression> Expressions { get; }

        List<ISQLinqTypedJoinExpression> JoinExpressions { get; }
        ITypedSqlLinq Parent { get; set; }
    }
}