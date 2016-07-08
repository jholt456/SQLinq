//Copyright (c) Chris Pietschmann 2012 (http://pietschsoft.com)
//Licensed under the GNU Library General Public License (LGPL)
//License can be found here: http://sqlinq.codeplex.com/license

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SQLinq.Compiler;

namespace SQLinq
{
    public class SQLinqJoinExpression<TOuter, TInner, TKey, TResult> : ISQLinqTypedJoinExpression
    {
        private readonly ISqlDialect dialect;

        public SQLinqJoinExpression(ISqlDialect dialect)
        {
            this.dialect = dialect;
        }

        public SQLinq<TInner> Inner { get; set; }
        public Expression<Func<TInner, TKey>> InnerKeySelector { get; set; }

        public SQLinq<TOuter> Outer { get; set; }
        public Expression<Func<TOuter, TKey>> OuterKeySelector { get; set; }
        public SQLinq<TResult> Results { get; set; }
        public Expression<Func<TOuter, TInner, TResult>> ResultSelector { get; set; }

        public SQLinqTypedJoinResult Process(IDictionary<string, object> parameters, string parameterNamePrefix = SqlExpressionCompiler.DefaultParameterNamePrefix)
        {
            var innerTable = this.Inner.GetTableName(true);

            var inner = this.Inner.ProcessJoinExpression(this.InnerKeySelector, parameterNamePrefix, parameters);
            var outer = this.Outer.ProcessJoinExpression(this.OuterKeySelector, parameterNamePrefix, parameters);
            var results = this.Outer.ProcessJoinExpression(this.ResultSelector, parameterNamePrefix, parameters);

            return new SQLinqTypedJoinResult(innerTable, inner, outer, results, parameters);
        }

        Expression ISQLinqTypedJoinExpression.OuterKeySelector
        {
            get { return this.OuterKeySelector; }
        }

        Expression ISQLinqTypedJoinExpression.InnerKeySelector
        {
            get { return this.InnerKeySelector; }
        }

        Expression ISQLinqTypedJoinExpression.ResultSelector
        {
            get { return this.ResultSelector; }
        }
    }
}