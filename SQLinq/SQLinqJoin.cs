using System.Linq;
using SQLinq.Compiler;

namespace SQLinq
{
    public class SQLinqJoin<TResult, TParent> : SQLinq<TResult>
    {
        public SQLinqJoin(SQLinq<TParent> parent) : base(parent.GetTableName(true), parent.Dialect)
        {
            this.Parent = parent;
        }

        public override ISQLinqResult ToSQL(int existingParameterCount = 0, string parameterNamePrefix = SqlExpressionCompiler.DefaultParameterNamePrefix)
        {
            var local = this as ITypedSqlLinq;

            var newItem = new SQLinq<TResult>(this.TableNameOverride, this.Dialect);

            while (local != null)
            {
                if (local.JoinExpressions.Any())
                {
                    newItem.JoinExpressions.AddRange(local.JoinExpressions);
                }

                if (local.Expressions.Any())
                {
                    newItem.Expressions.AddRange(local.Expressions);
                }

                local = local.Parent;
            }

            newItem.Select(this.Selector);

            newItem.JoinExpressions.Reverse();
            newItem.Expressions.Reverse();
            return newItem.ToSQL(existingParameterCount, parameterNamePrefix);
        }
    }
}