using System.Collections.Generic;
using System.Linq.Expressions;
using SQLinq.Compiler;

namespace SQLinq
{
    public interface ISQLinqTypedJoinExpression
    {
        SQLinqTypedJoinResult Process(IDictionary<string, object> parameters, string parameterNamePrefix = SqlExpressionCompiler.DefaultParameterNamePrefix);

        Expression OuterKeySelector { get; }
        Expression InnerKeySelector { get; }
        Expression ResultSelector { get; }
    }
}