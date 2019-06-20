﻿//Copyright (c) Chris Pietschmann 2013 (http://pietschsoft.com)
//Licensed under the GNU Library General Public License (LGPL)
//License can be found here: http://sqlinq.codeplex.com/license

using Microsoft.VisualStudio.TestTools.UnitTesting;
using SQLinq;
using SQLinq.Dynamic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SQLinq.Compiler;

namespace SQLinqTest
{
    [TestClass]
    public class SQLinqTest
    {
        [TestMethod]
        public void SQLinqTest_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.Age == 12
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] = @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[4]);
            Assert.AreEqual("[ParentID]", result.Select[5]);
            Assert.AreEqual("[Column With Spaces] AS [ColumnWithSpaces]", result.Select[6]);
        }

        [TestMethod]
        public void SQLinqTest_002()
        {
            var query = new SQLinq<Person>().Where(d => d.FirstName == "Chris");

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void SQLinqTest_003()
        {
            var query = new SQLinq<ICar>().Where(d => d.WheelDiameter == 15);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[WheelDiameter] = @sqlinq_1", result.Where);
            Assert.AreEqual(15, result.Parameters["@sqlinq_1"]);
        }


        [TestMethod]
        public void SQLinqTest_004()
        {
            var query = from d in new SQLinq<Person>("PersonTableOverride")
                        where d.Age == 12
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[PersonTableOverride]", result.Table);

            Assert.AreEqual("[Age] = @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[4]);
            Assert.AreEqual("[ParentID]", result.Select[5]);
            Assert.AreEqual("[Column With Spaces] AS [ColumnWithSpaces]", result.Select[6]);
        }

        [TestMethod]
        public void SQLinqTest_005()
        {
            var obj = new Person();

            var query = from d in obj.ToSQLinq()
                        where d.Age == 12 && d.FirstName.StartsWith("Ar")
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("([Age] = @sqlinq_1 AND [FirstName] LIKE @sqlinq_2)", result.Where);

            Assert.AreEqual(2, result.Parameters.Count);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Ar%", result.Parameters["@sqlinq_2"]);

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[4]);
            Assert.AreEqual("[ParentID]", result.Select[5]);
            Assert.AreEqual("[Column With Spaces] AS [ColumnWithSpaces]", result.Select[6]);
        }

        [TestMethod]
        public void SQLinqTest_006()
        {
            var query = from d in new SQLinq<Person>()
                        where d.Age < 104
                        select new
                        {
                            FirstName = d.FirstName.ToUpper()
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(104, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("UCASE([FirstName]) AS [FirstName]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_007()
        {
            var query = from d in new SQLinq<Person>()
                        where d.Age < 104
                        select d.FirstName.ToUpper();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(104, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("UCASE([FirstName])", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_008()
        {
            var query = from d in new SQLinq<Person>()
                        where d.Age < 104
                        select d.FirstName;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(104, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[FirstName]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_009()
        {
            var query = from d in new SQLinq<Person>()
                        where d.Age < 104
                        select d.FirstName.Length;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(104, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("LEN([FirstName])", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_010()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName == null
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[FirstName] IS NULL", result.Where);

            Assert.AreEqual(0, result.Parameters.Count);

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[4]);
            Assert.AreEqual("[ParentID]", result.Select[5]);
            Assert.AreEqual("[Column With Spaces] AS [ColumnWithSpaces]", result.Select[6]);
        }

        [TestMethod]
        public void SQLinqTest_011()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName != null
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[FirstName] IS NOT NULL", result.Where);

            Assert.AreEqual(0, result.Parameters.Count);

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[4]);
            Assert.AreEqual("[ParentID]", result.Select[5]);
            Assert.AreEqual("[Column With Spaces] AS [ColumnWithSpaces]", result.Select[6]);
        }

        [TestMethod]
        public void SQLinqTest_012()
        {
            string nullVal = null;
            var query = from d in new SQLinq<Person>()
                        where d.FirstName != nullVal
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[FirstName] IS NOT NULL", result.Where);

            Assert.AreEqual(0, result.Parameters.Count);

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[4]);
            Assert.AreEqual("[ParentID]", result.Select[5]);
            Assert.AreEqual("[Column With Spaces] AS [ColumnWithSpaces]", result.Select[6]);
        }

        [TestMethod]
        public void SQLinqTest_013()
        {
            string nullVal = null;
            var query = from d in new SQLinq<PersonFirstNameNoSelect>()
                        where d.FirstName != nullVal
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[PersonFirstNameNoSelect]", result.Table);

            Assert.AreEqual("[FirstName] IS NOT NULL", result.Where);

            Assert.AreEqual(0, result.Parameters.Count);

            Assert.AreEqual(6, result.Select.Length);
            Assert.IsFalse(result.Select.Any(x => x.Contains("FirstName", StringComparison.OrdinalIgnoreCase)));
        }

        #region DISTINCT

        [TestMethod]
        public void SQLinq_Distinct_001()
        {
            var query = from d in new SQLinq<Person>()
                        select d.FirstName;
            query = query.Distinct();

            var result = query.ToSQL();

            var code = result.ToQuery();

            Assert.AreEqual("SELECT DISTINCT [FirstName] FROM [Person]", code);
        }
        [TestMethod]
        public void SQLinq_Distinct_001_Queryable()
        {
            IQueryable<Person> people = new SQLinq<Person>();
            var query = from d in people
                        select d.FirstName;
            query = query.Distinct();

            var result = ((ITypedSqlLinq)query).ToSQL();

            var code = result.ToQuery();

            Assert.AreEqual("SELECT DISTINCT [FirstName] FROM [Person]", code);
        }

        [TestMethod]
        public void SQLinq_Distinct_Take_001()
        {
            var query = from d in new SQLinq<Person>()
                        select d.FirstName;
            query = query.Distinct();
            
            query = query.Take(10);

            var result = query.ToSQL();

            var code = result.ToQuery();

            Assert.AreEqual("SELECT DISTINCT TOP 10 [FirstName] FROM [Person]", code);
        }

        [TestMethod]
        public void SQLinq_Distinct_Skip_001()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.FirstName
                        select d.FirstName;
            query = query.Distinct();

            query = query.Skip(10);

            var result = query.ToSQL();

            var code = result.ToQuery();

            Assert.AreEqual("WITH SQLinq_data_set AS (SELECT DISTINCT [FirstName], ROW_NUMBER() OVER (ORDER BY [FirstName]) AS [SQLinq_row_number] FROM [Person]) SELECT * FROM SQLinq_data_set WHERE [SQLinq_row_number] >= 11 ORDER BY [SQLinq_row_number]", code);
        }

        [TestMethod]
        public void SQLinq_Distinct_SkipTake_001()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.FirstName
                        select d.FirstName;
            query = query.Distinct();

            query = query.Skip(20).Take(10);

            var result = query.ToSQL();

            var code = result.ToQuery();

            Assert.AreEqual("WITH SQLinq_data_set AS (SELECT DISTINCT [FirstName], ROW_NUMBER() OVER (ORDER BY [FirstName]) AS [SQLinq_row_number] FROM (SELECT DISTINCT [FirstName] FROM [Person]) AS d) SELECT * FROM SQLinq_data_set WHERE [SQLinq_row_number] BETWEEN 21 AND 30", code);
        }

        [TestMethod]
        public void SQLinq_Distinct_SkipTake_001_Queryable()
        {
            IQueryable<Person> people = new SQLinq<Person>();
            var query = from d in people
                        orderby d.FirstName
                        select d.FirstName;
            query = query.Distinct();

            query = query.Skip(20).Take(10);

            var result = ((ITypedSqlLinq)query).ToSQL();

            var code = result.ToQuery();

            Assert.AreEqual("WITH SQLinq_data_set AS (SELECT DISTINCT [FirstName], ROW_NUMBER() OVER (ORDER BY [FirstName]) AS [SQLinq_row_number] FROM (SELECT DISTINCT [FirstName] FROM [Person]) AS d) SELECT * FROM SQLinq_data_set WHERE [SQLinq_row_number] BETWEEN 21 AND 30", code);
        }

        #endregion

        #region ANONYMOUS TYPE

        private SQLinq<T> GetAnonSQLinq<T>(T anonymousType)
        {
            return new SQLinq<T>();
        }

        [TestMethod]
        public void AnonymousType_001()
        {
            var q = GetAnonSQLinq(new {
                FN = string.Empty,
                LN = string.Empty,
                A = 0
            });

            var query = from d in q
                        where d.FN.StartsWith("Ch")
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FN] LIKE @sqlinq_1", result.Where);
            Assert.AreEqual("Ch%", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void AnonymousType_002()
        {
            var obj = new
            {
                FirstName = string.Empty,
                LastName = string.Empty,
                MiddleName = string.Empty,
                Age = 0
            };

            var query = from d in SQLinq.SQLinq.Create(obj, "PersonTable")
                        where d.Age == 12 && d.MiddleName.StartsWith("R")
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[PersonTable]", result.Table);

            Assert.AreEqual("([Age] = @sqlinq_1 AND [MiddleName] LIKE @sqlinq_2)", result.Where);

            Assert.AreEqual(2, result.Parameters.Count);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("R%", result.Parameters["@sqlinq_2"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("*", result.Select[0]);
        }

        [TestMethod]
        public void AnonymousType_003()
        {
            var obj = new
            {
                FirstName = string.Empty,
                LastName = string.Empty,
                MiddleName = string.Empty,
                Age = 0
            };

            var query = from d in obj.ToSQLinq("PersonTable")
                        where d.Age == 12 && d.MiddleName.StartsWith("R")
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[PersonTable]", result.Table);

            Assert.AreEqual("([Age] = @sqlinq_1 AND [MiddleName] LIKE @sqlinq_2)", result.Where);

            Assert.AreEqual(2, result.Parameters.Count);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("R%", result.Parameters["@sqlinq_2"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("*", result.Select[0]);
        }

        #endregion

        #region SELECT

        [TestMethod]
        public void Select_001()
        {
            var query = new SQLinq<Person>().Select(d => d.Age);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[Age]", result.Select[0]);
        }

        [TestMethod]
        public void Select_002()
        {
            var query = from d in new SQLinq<Person>()
                        select d.Age;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[Age]", result.Select[0]);
        }

        [TestMethod]
        public void Select_003()
        {
            var query = from d in new SQLinq<Person>()
                        select new {
                            id = d.ID,
                            firstName = d.FirstName
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[ID] AS [id]", result.Select[0]);
            Assert.AreEqual("[FirstName] AS [firstName]", result.Select[1]);
        }

        [TestMethod]
        public void Select_004()
        {
            var query = from d in new SQLinq<Person>()
                        select new
                        {
                            id = d.ID,
                            fullname = d.FirstName + " " + d.LastName
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[ID] AS [id]", result.Select[0]);
            Assert.AreEqual("(([FirstName] + ' ') + [LastName]) AS [fullname]", result.Select[1]);
        }

        [TestMethod]
        public void Select_005()
        {
            var query = from d in new SQLinq<Person>()
                        select new
                        {
                            DoubleAge = d.Age * 2
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("([Age] * @sqlinq_1) AS [DoubleAge]", result.Select[0]);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Select_006()
        {
            var query = new SQLinq<Person>();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[4]);
            Assert.AreEqual("[ParentID]", result.Select[5]);
            Assert.AreEqual("[Column With Spaces] AS [ColumnWithSpaces]", result.Select[6]);
        }

        [TestMethod]
        public void Select_007()
        {
            var query = from d in new SQLinq<Person>()
                        select d.ColumnWithSpaces;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[Column With Spaces]", result.Select[0]);
        }

        [TestMethod]
        public void Select_008()
        {
            var query = from d in new SQLinq<Person>()
                        select d.ParentID;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[ParentID]", result.Select[0]);
        }

        [TestMethod]
        public void Select_009()
        {
            var query = from d in new SQLinq<Person>()
                        select d.IsEmployed;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[Is_Employed]", result.Select[0]);
        }

        [TestMethod]
        public void Select_10()
        {
            var query = from p in new SQLinq<Select_10_Class>()
                        select p.Status;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[Status]", result.Select[0]);
        }

        private class Select_10_Class
        {
            public Select_10_Enum Status { get; set; }
        }

        private enum Select_10_Enum
        {
            A,
            B,
            C
        }

        #endregion

        #region "WHERE EQUAL"

        [TestMethod]
        public void Where_Equal_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age == 12);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] = @sqlinq_1", result.Where);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Equal_002()
        {
            var query = new SQLinq<Person>().Where(d => d.FirstName == "Chris");

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Equal_003()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>().Where(d => d.ID == id);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[ID] = @sqlinq_1", result.Where);
            Assert.AreEqual(id, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Equal_004()
        {
            var id = Guid.NewGuid();
            var query = from d in new SQLinq<Person>()
                        where d.ID == id
                        select d;

            query = query.Where(d => d.FirstName == "Chris");

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[ID] = @sqlinq_1 AND [FirstName] = @sqlinq_2", result.Where);
            Assert.AreEqual(id, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_2"]);
        }

        [TestMethod, ExpectedException(typeof(Exception))]
        public void Where_Equal_005()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>();

            Func<Person, bool> q1 = d => d.ID == id;
            Func<Person, bool> q2 = d => d.FirstName == "Chris";

            // Exception: NodeType of "Invoke" not supported
            query.Where(d => q1(d) || q2(d));

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[ID] = @sqlinq_1 OR [FirstName] = @sqlinq_2", result.Where);
            Assert.AreEqual(id, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_2"]);
        }

        [TestMethod, ExpectedException(typeof(Exception))]
        public void Where_Equal_006()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>();

            var andCriteria = new List<Predicate<Person>>();
            andCriteria.Add(d => d.ID == id);
            andCriteria.Add(d => d.FirstName == "Chris");

            // Microsoft.CSharp.RuntimeBinder.RuntimeBinderException
            query.Where(d => andCriteria.All(p => p(d)));

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[ID] = @sqlinq_1 OR [FirstName] = @sqlinq_2", result.Where);
            Assert.AreEqual(id, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void Where_Equal_007()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>();

            Expression<Func<Person, bool>> q1 = d => d.ID == id;
            Expression<Func<Person, bool>> q2 = d => d.FirstName == "Chris";

            query.Where(Expression.Or(q1.Body, q2.Body));

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([ID] = @sqlinq_1 OR [FirstName] = @sqlinq_2)", result.Where);
            Assert.AreEqual(id, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void Where_Equal_008()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>();

            Expression<Func<Person, bool>> q1 = d => d.ID == id;
            Expression<Func<Person, bool>> q2 = d => d.FirstName == "Chris";

            query.Where(Expression.And(q1.Body, q2.Body));

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([ID] = @sqlinq_1 AND [FirstName] = @sqlinq_2)", result.Where);
            Assert.AreEqual(id, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void Where_Equal_009()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>();

            Expression<Func<Person, bool>> q1 = d => d.ID == id;
            Expression<Func<Person, bool>> q2 = d => d.FirstName == "Chris";

            query.Where(
                Expression.Or(
                    Expression.And(q1.Body, q2.Body),
                    Expression.And(q1.Body, q2.Body)
                    )
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("(([ID] = @sqlinq_1 AND [FirstName] = @sqlinq_2) OR ([ID] = @sqlinq_3 AND [FirstName] = @sqlinq_4))", result.Where);
            Assert.AreEqual(id, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_2"]);
            Assert.AreEqual(id, result.Parameters["@sqlinq_3"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_4"]);
        }

        [TestMethod]
        public void Where_Equal_010()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>();

            query.Where(
                Expression.Equal(
                    Expression.Multiply(
                        Expression.Constant(1),
                        Expression.Constant(2)
                        ),
                    Expression.Constant(2)
                )
            );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("(@sqlinq_1 * @sqlinq_2) = @sqlinq_3", result.Where);
            Assert.AreEqual(1, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(2, result.Parameters["@sqlinq_2"]);
            Assert.AreEqual(2, result.Parameters["@sqlinq_3"]);
        }

        public int Where_Equal_011_Value = 12;
        [TestMethod]
        public void Where_Equal_011()
        {
            var query = new SQLinq<Person>().Where(
                d => d.Age == this.Where_Equal_011_Value
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] = @sqlinq_1", result.Where);
            Assert.AreEqual(this.Where_Equal_011_Value, result.Parameters["@sqlinq_1"]);
        }

        private string Where_Equal_012_Value = "Chris";
        [TestMethod]
        public void Where_Equal_012()
        {
            var query = new SQLinq<Person>().Where(
                d => d.FirstName == this.Where_Equal_012_Value
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual(this.Where_Equal_012_Value, result.Parameters["@sqlinq_1"]);
        }

        public string Where_Equal_013_Value { get; set; }
        [TestMethod]
        public void Where_Equal_013()
        {
            this.Where_Equal_013_Value = "Chris";

            var query = new SQLinq<Person>().Where(
                d => d.FirstName == this.Where_Equal_013_Value
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual(this.Where_Equal_013_Value, result.Parameters["@sqlinq_1"]);
        }

        private string Where_Equal_014_Value { get; set; }
        [TestMethod]
        public void Where_Equal_014()
        {
            this.Where_Equal_014_Value = "Chris";

            var query = new SQLinq<Person>().Where(
                d => d.FirstName == this.Where_Equal_014_Value
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual(this.Where_Equal_014_Value, result.Parameters["@sqlinq_1"]);
        }

        public string Where_Equal_015_Value { get; private set; }
        [TestMethod]
        public void Where_Equal_015()
        {
            this.Where_Equal_015_Value = "Chris";

            var query = new SQLinq<Person>().Where(
                d => d.FirstName == this.Where_Equal_015_Value
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual(this.Where_Equal_015_Value, result.Parameters["@sqlinq_1"]);
        }

        public const string Where_Equal_016_Value = "Chris";
        [TestMethod]
        public void Where_Equal_016()
        {
            var query = new SQLinq<Person>().Where(
                d => d.FirstName == Where_Equal_016_Value
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual(Where_Equal_016_Value, result.Parameters["@sqlinq_1"]);
        }

        private const string Where_Equal_017_Value = "Chris";
        [TestMethod]
        public void Where_Equal_017()
        {
            var query = new SQLinq<Person>().Where(
                d => d.FirstName == Where_Equal_017_Value
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual(Where_Equal_017_Value, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Equal_018()
        {
            var val = new { Name = "Chris" };

            var query = new SQLinq<Person>().Where(
                d => d.FirstName == val.Name
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual(val.Name, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Equal_019()
        {
            var val = new
            {
                Person = new { Name = "Chris" }
            };

            var query = new SQLinq<Person>().Where(
                d => d.FirstName == val.Person.Name
                );

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
            Assert.AreEqual(val.Person.Name, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Equal_020()
        {
            var query = new SQLinq<Person>().Where(d => d.FirstName == " ");

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(0, result.Parameters.Count);
            Assert.AreEqual("[FirstName] = ' '", result.Where);
        }

        [TestMethod]
        public void Where_Equal_021()
        {
            var query = new SQLinq<Person>().Where(d => d.FirstName == null);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(0, result.Parameters.Count);
            Assert.AreEqual("[FirstName] IS NULL", result.Where);
        }

        [TestMethod]
        public void Where_Equal_022()
        {
            string val = " ";
            var query = new SQLinq<Person>().Where(d => d.FirstName == val);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(0, result.Parameters.Count);
            Assert.AreEqual("[FirstName] = ' '", result.Where);
        }

        [TestMethod]
        public void Where_Equal_023()
        {
            string val = null;
            var query = new SQLinq<Person>().Where(d => d.FirstName == val);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(0, result.Parameters.Count);
            Assert.AreEqual("[FirstName] IS NULL", result.Where);
        }

        [TestMethod]
        public void Where_Equal_024_A()
        {
            var val = new Where_Equal_024_Object { Value = null };
            var query = new SQLinq<Person>().Where(d => d.FirstName == val.Value);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(0, result.Parameters.Count);
            Assert.AreEqual("[FirstName] IS NULL", result.Where);
        }

        [TestMethod]
        public void Where_Equal_024_B()
        {
            var val = new Where_Equal_024_Object { Value = " " };
            var query = new SQLinq<Person>().Where(d => d.FirstName == val.Value);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(0, result.Parameters.Count);
            Assert.AreEqual("[FirstName] = ' '", result.Where);
        }

        public class Where_Equal_024_Object
        {
            public string Value { get; set; }
        }

        [TestMethod]
        public void Where_Equal_NewGuid_001()
        {
            var query = new SQLinq<Person>().Where(d => d.ID == Guid.NewGuid());

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[ID] = @sqlinq_1", result.Where);
            
            var g = (Guid)result.Parameters["@sqlinq_1"];
        }

        #endregion

        #region WHERE STRING OPERATIONS

        [TestMethod]
        public void Where_String_001()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>().Where(d => d.FirstName.StartsWith("Ch"));

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] LIKE @sqlinq_1", result.Where);
            Assert.AreEqual("Ch%", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_String_002()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>().Where(d => d.FirstName.EndsWith("is"));

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] LIKE @sqlinq_1", result.Where);
            Assert.AreEqual("%is", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_String_003()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>().Where(d => d.FirstName.Contains("hri"));

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] LIKE @sqlinq_1", result.Where);
            Assert.AreEqual("%hri%", result.Parameters["@sqlinq_1"]);
        }



        #endregion

        #region "WHERE NOT EQUAL"

        [TestMethod]
        public void Where_NotEqual_001()
        {
            var id = Guid.NewGuid();
            var query = new SQLinq<Person>().Where(d => d.ID != id);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[ID] <> @sqlinq_1", result.Where);
            Assert.AreEqual(id, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_NotEqual_002()
        {
            var query = new SQLinq<Person>().Where(d => d.Age != 34);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] <> @sqlinq_1", result.Where);
            Assert.AreEqual(34, result.Parameters["@sqlinq_1"]);
        }

        #endregion

        #region WHERE GREATER THAN

        [TestMethod]
        public void Where_GreaterThan_001()
        {
            var age = 12;
            var query = new SQLinq<Person>().Where(d => d.Age > age);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] > @sqlinq_1", result.Where);
            Assert.AreEqual(age, result.Parameters["@sqlinq_1"]);
        }

        #endregion

        #region WHERE LESS THAN

        [TestMethod]
        public void Where_LessThan_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age < 65);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);
            Assert.AreEqual(65, result.Parameters["@sqlinq_1"]);
        }

        #endregion

        #region WHERE GREATER THAN OR EQUAL

        [TestMethod]
        public void Where_GreaterThanOrEqual_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age >= 22);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] >= @sqlinq_1", result.Where);
            Assert.AreEqual(22, result.Parameters["@sqlinq_1"]);
        }

        #endregion

        #region WHERE LESS THAN OR EQUAL

        [TestMethod]
        public void Where_LessThanOrEqual_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age <= 24);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] <= @sqlinq_1", result.Where);
            Assert.AreEqual(24, result.Parameters["@sqlinq_1"]);
        }

        #endregion

        #region WHERE MULTIPLY

        [TestMethod]
        public void Where_Multiply_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age * 2 == 30);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Age] * @sqlinq_1) = @sqlinq_2", result.Where);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(30, result.Parameters["@sqlinq_2"]);
        }

        #endregion

        #region WHERE DIVIDE

        [TestMethod]
        public void Where_Divide_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age / 2 == 30);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Age] / @sqlinq_1) = @sqlinq_2", result.Where);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(30, result.Parameters["@sqlinq_2"]);
        }

        #endregion

        #region WHERE ADD

        [TestMethod]
        public void Where_Add_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age + 2 == 30);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Age] + @sqlinq_1) = @sqlinq_2", result.Where);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(30, result.Parameters["@sqlinq_2"]);
        }

        #endregion

        #region WHERE SUBTRACT

        [TestMethod]
        public void Where_Subtract_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age - 2 == 30);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Age] - @sqlinq_1) = @sqlinq_2", result.Where);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(30, result.Parameters["@sqlinq_2"]);
        }

        #endregion

        #region WHERE MODULUS

        [TestMethod]
        public void Where_Modulus_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age % 2 == 30);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Age] % @sqlinq_1) = @sqlinq_2", result.Where);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(30, result.Parameters["@sqlinq_2"]);
        }

        #endregion

        #region WHERE COMPLEX EXAMPLES

        [TestMethod]
        public void Where_Complex_001()
        {
            var query = new SQLinq<Person>().Where(d => d.Age == 13 && d.Age < 20);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Age] = @sqlinq_1 AND [Age] < @sqlinq_2)", result.Where);
            Assert.AreEqual(13, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(20, result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void Where_Complex_002()
        {
            var query = new SQLinq<Person>().Where(d => d.Age == 13 || d.Age < 20);

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Age] = @sqlinq_1 OR [Age] < @sqlinq_2)", result.Where);
            Assert.AreEqual(13, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(20, result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void Where_Complex_003()
        {
            var query = new SQLinq<Person>().Where(d => (d.Age == 13 || d.Age < 20) && (d.Age == 50 || d.Age > 40));

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("(([Age] = @sqlinq_1 OR [Age] < @sqlinq_2) AND ([Age] = @sqlinq_3 OR [Age] > @sqlinq_4))", result.Where);
            Assert.AreEqual(13, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(20, result.Parameters["@sqlinq_2"]);
            Assert.AreEqual(50, result.Parameters["@sqlinq_3"]);
            Assert.AreEqual(40, result.Parameters["@sqlinq_4"]);
        }

        [TestMethod]
        public void Where_Complex_004()
        {
            var query = new SQLinq<Person>().Where(d => (d.Age == 13 || d.Age < 20) && (d.Age == 50 || d.Age > 40));

            var result = (SQLinqSelectResult)query.ToSQL(5);

            Assert.AreEqual("(([Age] = @sqlinq_6 OR [Age] < @sqlinq_7) AND ([Age] = @sqlinq_8 OR [Age] > @sqlinq_9))", result.Where);
            Assert.AreEqual(13, result.Parameters["@sqlinq_6"]);
            Assert.AreEqual(20, result.Parameters["@sqlinq_7"]);
            Assert.AreEqual(50, result.Parameters["@sqlinq_8"]);
            Assert.AreEqual(40, result.Parameters["@sqlinq_9"]);
        }

        #endregion

        #region WHERE USING ENUM

        [TestMethod]
        public void Where_Enum_001()
        {
            var query = from p in new SQLinq<Where_Enum_Class_001>()
                        where p.Status == SQLinqTest.Where_Enum_Enum_001.FirstValue
                        select p;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Status] = @sqlinq_1", result.Where);
            Assert.AreEqual(SQLinqTest.Where_Enum_Enum_001.FirstValue, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Oracle_Enum_001()
        {
            var dialect = new OracleDialect();
            var query = from p in new SQLinq<Where_Enum_Class_001>(dialect)
                        where p.Status == SQLinqTest.Where_Enum_Enum_001.FirstValue
                        select p;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("Status = :sqlinq_1", result.Where);
            Assert.AreEqual(SQLinqTest.Where_Enum_Enum_001.FirstValue, result.Parameters[":sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Enum_002()
        {
            var query = from p in new SQLinq<Where_Enum_Class_001>()
                        where p.Status == SQLinqTest.Where_Enum_Enum_001.SecondValue
                        select p;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Status] = @sqlinq_1", result.Where);
            Assert.AreEqual(SQLinqTest.Where_Enum_Enum_001.SecondValue, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Where_Oracle_Enum_002()
        {
            var dialect = new OracleDialect();
            var query = from p in new SQLinq<Where_Enum_Class_001>(dialect)
                        where p.Status == SQLinqTest.Where_Enum_Enum_001.SecondValue
                        select p;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("Status = :sqlinq_1", result.Where);
            Assert.AreEqual(SQLinqTest.Where_Enum_Enum_001.SecondValue, result.Parameters[":sqlinq_1"]);
        }

        private class Where_Enum_Class_001
        {
            public Where_Enum_Enum_001 Status { get; set; }
        }

        private enum Where_Enum_Enum_001 : int
        {
            FirstValue = 0,
            SecondValue = 1
        }

        [TestMethod]
        public void Where_Enum_003()
        {
            var query = from p in new SQLinq<Where_Enum_Class_003>()
                        where p.Status == SQLinqTest.Where_Enum_Enum_003.C
                        select p;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Status] = @sqlinq_1", result.Where);
            Assert.AreEqual(SQLinqTest.Where_Enum_Enum_003.C, result.Parameters["@sqlinq_1"]);
        }

        private class Where_Enum_Class_003
        {
            public Where_Enum_Enum_003 Status { get; set; }
        }

        private enum Where_Enum_Enum_003
        {
            A,
            B,
            C
        }

        #endregion

        #region Table And Column Attributes

        [TestMethod]
        public void SQLinqTest_TableAndColumnAttribute_001()
        {
            var query = from d in new SQLinq<PersonView>()
                        where d.FirstName.StartsWith("C") && d.Age == 12
                        select new
                        {
                            FirstName = d.FirstName,
                            LastName = d.LastName
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[vw_Person]", result.Table);

            Assert.AreEqual("([First_Name] LIKE @sqlinq_1 AND [Age] = @sqlinq_2)", result.Where);

            Assert.AreEqual(2, result.Parameters.Count);
            Assert.AreEqual("C%", result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(12, result.Parameters["@sqlinq_2"]);

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[First_Name] AS [FirstName]", result.Select[0]);
            Assert.AreEqual("[Last_Name] AS [LastName]", result.Select[1]);
        }

        [TestMethod]
        public void SQLinqTest_TableAndColumnAttribute_002()
        {
            var query = from d in new SQLinq<ICar2>()
                        where d.WheelDiameter > 14
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Car]", result.Table);

            Assert.AreEqual("[Wheel_Diameter] > @sqlinq_1", result.Where);
            Assert.AreEqual(14, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[Wheel_Diameter] AS [WheelDiameter]", result.Select[0]);
        }

        #endregion

        #region Sub-Query

        [TestMethod]
        public void SQLinqTest_SubQuery_001()
        {
            var query = from p in new SQLinq<SubQueryPerson>()
                        where p.FirstName == "Chris"
                        select p;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("*", result.Select[0]);

            Assert.AreEqual("(SELECT [ID], [FirstName], [LastName], [Age] FROM [Person] WHERE [Age] > @sqlinq_1) AS [SubQueryPerson]", result.Table);

            Assert.AreEqual("[FirstName] = @sqlinq_2", result.Where);

            Assert.AreEqual(30, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void SQLinqTest_SubQuery_002()
        {
            var query = from p in new SQLinq<SubQueryPerson2>()
                        where p.FirstName == "Chris"
                        select p;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(5, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Desc] AS [Description]", result.Select[4]);

            Assert.AreEqual("(SELECT ID, FirstName, LastName, Age, Desc FROM StraightSQLPerson) AS [Person]", result.Table);

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);

            Assert.AreEqual("Chris", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void SQLinqTest_SubQuery_003()
        {
            var query = from p in new SQLinq<SubQueryPerson2>()
                        where p.FirstName == "Steve" && p.Description == "test"
                        select p;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual(5, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Desc] AS [Description]", result.Select[4]);

            Assert.AreEqual("(SELECT ID, FirstName, LastName, Age, Desc FROM StraightSQLPerson) AS [Person]", result.Table);

            Assert.AreEqual("([FirstName] = @sqlinq_1 AND [Desc] = @sqlinq_2)", result.Where);

            Assert.AreEqual("Steve", result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("test", result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void SQLinqTest_SubQuery_004()
        {
            IQueryable<Person> persons = new SQLinq<Person>();
            persons = persons.Where(x => x.IsEmployed == true);
            var query = from p in (from item in persons select item)
                        where p.FirstName == "Steve" 
                        select p;

            var result = (SQLinqSelectResult)((ITypedSqlLinq)query).ToSQL();
            Assert.AreEqual("(SELECT ID, FirstName, LastName, Age, Desc FROM (SELECT * FROM [Person] WHERE [Is_Employed] = 1) AS [Person]", result.ToQuery());
            Assert.AreEqual(5, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[LastName]", result.Select[2]);
            Assert.AreEqual("[Age]", result.Select[3]);
            Assert.AreEqual("[Desc] AS [Description]", result.Select[4]);

            Assert.AreEqual("(SELECT ID, FirstName, LastName, Age, Desc FROM (SELECT * FROM [Person] WHERE [Is_Employed] = 1) AS [Person]", result.Table);

            Assert.AreEqual("([FirstName] = @sqlinq_1 AND [Desc] = @sqlinq_2)", result.Where);

            Assert.AreEqual("Steve", result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("test", result.Parameters["@sqlinq_2"]);
        }

        #endregion

        #region TAKE

        [TestMethod]
        public void SQLinqTest_Take_001()
        {
            var query = from d in new SQLinq<Person>()
                        select d;

            var result = query.Take(15).ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT TOP 15 [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person]", sql);
        }

        [TestMethod]
        public void SQLinqTest_Take_Child_001()
        {
            var query = new SQLinq<Person>().Select(x => new {x.FirstName});

            var result = query.Take(15).ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT TOP 15 [FirstName] FROM [Person]", sql);
        }

        [TestMethod]
        public void SQLinqTest_Take_Child_002()
        {
            IQueryable<Person> query = new SQLinq<Person>().Select(x => new { x.FirstName });

            var result = (ITypedSqlLinq)query.Take(15);

            var sql = result.ToSQL().ToQuery();

            Assert.AreEqual("SELECT TOP 15 [FirstName] FROM [Person]", sql);
        }

        [TestMethod]
        public void SQLinqTest_Take_Child_003()
        {
            IQueryable<Person> query = new SQLinq<Person>();

            var result = (ITypedSqlLinq)query.Take(15);

            var sql = result.ToSQL().ToQuery();

            Assert.AreEqual("SELECT TOP 15 [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person]", sql);
        }

        #endregion

        #region SKIP

        [TestMethod]
        public void SQLinqTest_Skip_001()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.ID
                        select d;

            var result = query.Skip(15).ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("WITH SQLinq_data_set AS (SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces], ROW_NUMBER() OVER (ORDER BY [ID]) AS [SQLinq_row_number] FROM [Person]) SELECT * FROM SQLinq_data_set WHERE [SQLinq_row_number] >= 16", sql);
        }

        [TestMethod]
        public void SQLinqTest_Skip_002()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.FirstName
                        select d;

            var result = query.Skip(5).ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("WITH SQLinq_data_set AS (SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces], ROW_NUMBER() OVER (ORDER BY [FirstName]) AS [SQLinq_row_number] FROM [Person]) SELECT * FROM SQLinq_data_set WHERE [SQLinq_row_number] >= 6", sql);
        }

        [TestMethod]
        public void SQLinqTest_Skip_003()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.Age
                        select d;

            var result = query.Skip(10).Take(5).ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("WITH SQLinq_data_set AS (SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces], ROW_NUMBER() OVER (ORDER BY [Age]) AS [SQLinq_row_number] FROM [Person]) SELECT * FROM SQLinq_data_set WHERE [SQLinq_row_number] BETWEEN 11 AND 15", sql);
        }

        [TestMethod]
        public void SQLinqTest_Skip_005()
        {
            IQueryable<Person> query = new SQLinq<Person>().OrderBy(x => x.FirstName);


             query = query.Skip(10).Take(5);
            var result = ((ITypedSqlLinq) query).ToSQL();
            var sql = result.ToQuery();

            Assert.AreEqual("WITH SQLinq_data_set AS (SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces], ROW_NUMBER() OVER (ORDER BY [Age]) AS [SQLinq_row_number] FROM [Person]) SELECT * FROM SQLinq_data_set WHERE [SQLinq_row_number] BETWEEN 11 AND 15", sql);
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void SQLinqTest_Skip_004()
        {
            var query = from d in new SQLinq<Person>()
                        select d;

            var result = query.Skip(15).ToSQL();

            var sql = result.ToQuery();
        }

        #endregion

        #region ORDER BY

        [TestMethod]
        public void SQLinqTest_OrderBy_001()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.FirstName
                        select d;

            var result = query.ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person] ORDER BY [FirstName]", sql);
        }

        [TestMethod]
        public void SQLinqTest_OrderBy_002()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.FirstName, d.LastName
                        select d;

            var result = query.ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person] ORDER BY [FirstName], [LastName]", sql);
        }

        [TestMethod]
        public void SQLinqTest_OrderBy_003()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.FirstName, d.LastName
                        select d;

            query.OrderBy(d => d.Age);

            var result = query.ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person] ORDER BY [Age]", sql);
        }

        [TestMethod]
        public void SQLinqTest_OrderBy_004()
        {
            var query = from d in new SQLinq<Person>()
                        orderby 1
                        select d;

            var result = query.ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person] ORDER BY @sqlinq_1", sql);
        }

        [TestMethod]
        public void SQLinqTest_OrderBy_005()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.Age descending
                        select d;

            var result = query.ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person] ORDER BY [Age] DESC", sql);
        }

        [TestMethod]
        public void SQLinqTest_OrderBy_006()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.FirstName descending, d.Age
                        select d;

            var result = query.ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person] ORDER BY [FirstName] DESC, [Age]", sql);
        }

        [TestMethod]
        public void SQLinqTest_OrderBy_007()
        {
            var query = from d in new SQLinq<Person>()
                        orderby d.FirstName descending, d.Age, d.LastName descending, d.ID descending, d.ParentID
                        select d;

            var result = query.ToSQL();

            var sql = result.ToQuery();

            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person] ORDER BY [FirstName] DESC, [Age], [LastName] DESC, [ID] DESC, [ParentID]", sql);
        }

        #endregion

        #region STRING METHODS

        [TestMethod]
        public void String_StartsWith_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.StartsWith("C")
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] LIKE @sqlinq_1", result.Where);
            Assert.AreEqual("C%", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_EndsWith_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.EndsWith("C")
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] LIKE @sqlinq_1", result.Where);
            Assert.AreEqual("%C", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_Length_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Length == 4
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("LEN([FirstName]) = @sqlinq_1", result.Where);
            Assert.AreEqual(4, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_Length_002()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Length > 4
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("LEN([FirstName]) > @sqlinq_1", result.Where);
            Assert.AreEqual(4, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_Length_003()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Length < 4
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("LEN([FirstName]) < @sqlinq_1", result.Where);
            Assert.AreEqual(4, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_Length_004()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Length >= 4
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("LEN([FirstName]) >= @sqlinq_1", result.Where);
            Assert.AreEqual(4, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_Length_005()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Length <= 4
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("LEN([FirstName]) <= @sqlinq_1", result.Where);
            Assert.AreEqual(4, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_ToUpper_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.ToUpper() == "CHRIS"
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("UCASE([FirstName]) = @sqlinq_1", result.Where);
            Assert.AreEqual("CHRIS", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_ToLower_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.ToLower() == "CHRIS"
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("LCASE([FirstName]) = @sqlinq_1", result.Where);
            Assert.AreEqual("CHRIS", result.Parameters["@sqlinq_1"]);
        }


        [TestMethod]
        public void String_Substring_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Substring(0, 5) == "CHRIS"
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SUBSTR([FirstName], @sqlinq_1, @sqlinq_2) = @sqlinq_3", result.Where);
            Assert.AreEqual(0, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(5, result.Parameters["@sqlinq_2"]);
            Assert.AreEqual("CHRIS", result.Parameters["@sqlinq_3"]);
        }

        [TestMethod]
        public void String_Substring_002()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Substring(6) == "CHRIS"
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SUBSTR([FirstName], @sqlinq_1) = @sqlinq_2", result.Where);
            Assert.AreEqual(6, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("CHRIS", result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void String_IndexOf_001()
        {
            // as if [FirstName] == 'CHRIS' then will be True
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.IndexOf("HR") == 2
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("CHARINDEX(@sqlinq_1, [FirstName]) = @sqlinq_2", result.Where);
            Assert.AreEqual("HR", result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(2, result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void String_IndexOf_002()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.IndexOf("HR") != 0
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("CHARINDEX(@sqlinq_1, [FirstName]) <> @sqlinq_2", result.Where);
            Assert.AreEqual("HR", result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(0, result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void String_Trim_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Trim() == "Chris"
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("LTRIM(RTRIM([FirstName])) = @sqlinq_1", result.Where);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void String_Replace_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.FirstName.Replace("Chr", "Cr") == "Chris"
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("REPLACE([FirstName], @sqlinq_1, @sqlinq_2) = @sqlinq_3", result.Where);
            Assert.AreEqual("Chr", result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Cr", result.Parameters["@sqlinq_2"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_3"]);
        }

        [TestMethod]
        public void String_Replace_002()
        {
            IQueryable<Person> query = new SQLinq<Person>();
            query = query.Where(x=>x.FirstName !=null).Where(x =>    x.IsEmployed || x.FirstName.Replace("Chr", "Cr") == "Chris");


            var result = (SQLinqSelectResult)((ITypedSqlLinq) query).ToSQL();

            Assert.AreEqual("[FirstName] IS NOT NULL AND ([Is_Employed] OR REPLACE([FirstName], @sqlinq_1, @sqlinq_2) = @sqlinq_3)", result.Where);
            Assert.AreEqual("Chr", result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Cr", result.Parameters["@sqlinq_2"]);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_3"]);
        }

        #endregion

        #region INT METHODS

        [TestMethod]
        public void Int_Test_001()
        {
            var query = from d in new SQLinq<Person>()
                        where d.Age + 1 == 2
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Age] + @sqlinq_1) = @sqlinq_2", result.Where);
            Assert.AreEqual(1, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(2, result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void Int_Test_002()
        {
            var query = from d in new SQLinq<GenericTypeTestClass>()
                        where d.Price.Value > 2
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Price] > @sqlinq_1", result.Where);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Int_Test_002_GVOD_SQL()
        {
            var query = from d in new SQLinq<GenericTypeTestClass>()
                        where d.Price.GetValueOrDefault() > 2
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("ISNULL([Price],@defaultValue_0) > @sqlinq_1", result.Where);
            Assert.AreEqual(0, result.Parameters["@defaultValue_0"]);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Int_Test_002_GVOD_ORACLE()
        {
            var query = from d in new SQLinq<GenericTypeTestClass>(new OracleDialect())
                        where d.Price.GetValueOrDefault() > 2
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("NVL(Price,:defaultValue_0) > :sqlinq_1", result.Where);
            Assert.AreEqual(0, result.Parameters[":defaultValue_0"]);
            Assert.AreEqual(2, result.Parameters[":sqlinq_1"]);
        }

        [TestMethod]
        public void Int_Test_003()
        {
            var query = from d in new SQLinq<GenericTypeTestClass>()
                        where d.Price.HasValue && d.Price.Value > 2
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Price] IS NOT NULL AND [Price] > @sqlinq_1)", result.Where);
            Assert.AreEqual(2, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Int_Oracle_Test_001()
        {
            var dialect = new OracleDialect();
            var query = from d in new SQLinq<Person>(dialect)
                        where d.Age + 1 == 2
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("(Age + :sqlinq_1) = :sqlinq_2", result.Where);
            Assert.AreEqual(1, result.Parameters[":sqlinq_1"]);
            Assert.AreEqual(2, result.Parameters[":sqlinq_2"]);
        }

        #endregion

        #region BOOLEAN METHODS

        [TestMethod]
        public void Bool_Test_001()
        {
            var query = from d in new SQLinq<Person>()
                        where true == false
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("@sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(false, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Bool_Test_002()
        {
            var test = true;
            var query = from d in new SQLinq<Person>()
                        where test == false
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("@sqlinq_1 = @sqlinq_2", result.Where);
            Assert.AreEqual(2, result.Parameters.Count);
            Assert.AreEqual(true, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(false, result.Parameters["@sqlinq_2"]);
        }

        [TestMethod]
        public void Bool_Test_003()
        {
            var test = true;
            var query = from d in new SQLinq<Person>()
                        where test == false || d.IsEmployed == test
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("(@sqlinq_1 = @sqlinq_2 OR [Is_Employed] = @sqlinq_3)", result.Where);
            Assert.AreEqual(3, result.Parameters.Count);
            Assert.AreEqual(true, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual(false, result.Parameters["@sqlinq_2"]);
            Assert.AreEqual(true, result.Parameters["@sqlinq_3"]);
        }

        #endregion
        
        #region GUID METHODS

        [TestMethod]
        public void Guid_Test_001()
        {
            var test = Guid.Empty;
            var query = from d in new SQLinq<Person>()
                        where d.ParentID != test
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[ParentID] <> @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(Guid.Empty, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Guid_Test_002()
        {
            var test = Guid.NewGuid();
            var query = from d in new SQLinq<Person>()
                        where d.ParentID != test
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[ParentID] <> @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        #endregion

        #region DATETIME METHODS

        [TestMethod]
        public void DateTime_001()
        {
            var test = DateTime.Now;
            var query = from d in new SQLinq<DateTime_001_Class>()
                        where d.Date > test
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_002()
        {
            var query = from d in new SQLinq<DateTime_001_Class>()
                        where d.Date > DateTime.Now
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);

            var dt = (DateTime)result.Parameters["@sqlinq_1"];
            Assert.AreEqual(DateTime.Now.ToString("f"), dt.ToString("f"));            
        }

        [TestMethod]
        public void DateTime_003()
        {
            var query = from d in new SQLinq<DateTime_001_Class>()
                        where d.Date > DateTime.UtcNow
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);

            var dt = (DateTime)result.Parameters["@sqlinq_1"];
            Assert.AreEqual(DateTime.UtcNow.ToString("f"), dt.ToString("f"));
        }

        private class DateTime_001_Class
        {
            public int ID { get; set; }
            public DateTime Date { get; set; }
        }

        [TestMethod]
        public void DateTime_004()
        {
            var test = DateTime.Now;
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date > test
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_005()
        {
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date > DateTime.Now
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);

            var dt = (DateTime)result.Parameters["@sqlinq_1"];
            Assert.AreEqual(DateTime.Now.ToString("f"), dt.ToString("f"));
        }

        [TestMethod]
        public void DateTime_006()
        {
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date > DateTime.UtcNow
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);

            var dt = (DateTime)result.Parameters["@sqlinq_1"];
            Assert.AreEqual(DateTime.UtcNow.ToString("f"), dt.ToString("f"));
        }

        [TestMethod]
        public void DateTime_007()
        {
            var test = DateTime.Now;
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date.Value > test
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_008()
        {
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date.Value > DateTime.Now
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);

            var dt = (DateTime)result.Parameters["@sqlinq_1"];
            Assert.AreEqual(DateTime.Now.ToString("f"), dt.ToString("f"));
        }

        [TestMethod]
        public void DateTime_009()
        {
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date.Value > DateTime.UtcNow
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Date] > @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);

            var dt = (DateTime)result.Parameters["@sqlinq_1"];
            Assert.AreEqual(DateTime.UtcNow.ToString("f"), dt.ToString("f"));
        }

        [TestMethod]
        public void DateTime_010()
        {
            var test = DateTime.Now;
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date.HasValue && d.Date.Value > test
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Date] IS NOT NULL AND [Date] > @sqlinq_1)", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_010_Queryable()
        {
            var test = DateTime.Now;
            IQueryable<DateTime_002_Class> dateTime002Classes = new SQLinq<DateTime_002_Class>();
            var query = from d in dateTime002Classes
                        where d.Date.HasValue && d.Date.Value > test
                        select d;

            var result = (SQLinqSelectResult)((ITypedSqlLinq)query).ToSQL();

            Assert.AreEqual("([Date] IS NOT NULL AND [Date] > @sqlinq_1)", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_010_Join()
        {
            var test = DateTime.Now;
            var query = from d in new SQLinq<DateTime_002_Class>()
                        join d2 in new SQLinq<DateTime_002_Class>() on d.ID equals d2.ID
                        where d.Date.HasValue && d.Date.Value > test
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([d].[Date] IS NOT NULL AND [d].[Date] > @sqlinq_1)", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_010_Join_Group()
        {
            var test = DateTime.Now;
            var query = from d in new SQLinq<DateTime_002_Class>()
                        join d2 in new SQLinq<DateTime_002_Class>() on d.ID equals d2.ID
                        where d.Date.HasValue && d.Date.Value > test
                        group d by d.Date into f
                        select new
                               {
                                   f.Key
                               };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([d].[Date] IS NOT NULL AND [d].[Date] > @sqlinq_1)", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_010_Join_Queryable()
        {
            var test = DateTime.Now;
            IQueryable<DateTime_002_Class> dateTime002Classes = new SQLinq<DateTime_002_Class>();
            var query = from d in dateTime002Classes
                        join d2 in new SQLinq<DateTime_002_Class>() on d.ID equals d2.ID
                        where d.Date.HasValue && d.Date.Value > test
                        select d;

            var result = (SQLinqSelectResult)((ITypedSqlLinq)query).ToSQL();

            Assert.AreEqual("([d].[Date] IS NOT NULL AND [d].[Date] > @sqlinq_1)", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_010_Join_Group_Queryable()
        {
            var test = DateTime.Now;
            IQueryable<DateTime_002_Class> dateTime002Classes = new SQLinq<DateTime_002_Class>();
            var query = from d in dateTime002Classes
                        join d2 in new SQLinq<DateTime_002_Class>() on d.ID equals d2.ID
                        where d.Date.HasValue && d.Date.Value > test
                        group d by d.Date into f
                        select new
                        {
                            f.Key
                        };

            var result = (SQLinqSelectResult)((ITypedSqlLinq)query).ToSQL();

            Assert.AreEqual("([d].[Date] IS NOT NULL AND [d].[Date] > @sqlinq_1)", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void DateTime_011()
        {
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date.HasValue && d.Date.Value > DateTime.Now
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Date] IS NOT NULL AND [Date] > @sqlinq_1)", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);

            var dt = (DateTime)result.Parameters["@sqlinq_1"];
            Assert.AreEqual(DateTime.Now.ToString("f"), dt.ToString("f"));
        }

        [TestMethod]
        public void DateTime_012()
        {
            var query = from d in new SQLinq<DateTime_002_Class>()
                        where d.Date.HasValue && d.Date.Value > DateTime.UtcNow
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("([Date] IS NOT NULL AND [Date] > @sqlinq_1)", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);

            var dt = (DateTime)result.Parameters["@sqlinq_1"];
            Assert.AreEqual(DateTime.UtcNow.ToString("f"), dt.ToString("f"));
        }

        private class DateTime_002_Class
        {
            public int ID { get; set; }
            public DateTime? Date { get; set; }
        }

        #endregion

        #region Enumerable 
        [TestMethod]
        public void Enumerable_001()
        {
            var test = new int[] {1, 2, 3};
            var query = from d in new SQLinq<Person>()
                        where test.Contains(d.Age)
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] IN @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual( (IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Enumerable_002()
        {
            var test = new List<int> { 1, 2, 3 };
            var query = from d in new SQLinq<Person>()
                        where test.Contains(d.Age)
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] IN @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Enumerable_003()
        {
            var test = new Stack<int>(new[] {1, 2, 3});
            var query = from d in new SQLinq<Person>()
                        where test.Contains(d.Age)
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] IN @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Enumerable_004()
        {
            var test = (new[] { 1, 2, 3 }).AsQueryable();
            var query = from d in new SQLinq<Person>()
                        
                        where test.Contains(d.Age)
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Age] IN @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Enumerable_005()
        {
            var test = (new[] { 1, 2, 3 }).AsQueryable().ToList();
            var query = from d in new SQLinq<Person>()
                        join c in new SQLinq<ParentPerson>() on d.Age equals c.Age
                        where test.Contains(d.Age)
                        select d;

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[d].[Age] IN @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void Enumerable_006()
        {
            var test = (new[] { 1, 2, 3 }).ToList();
            IQueryable<Person> query = (new SQLinq<Person>());
            query = query.Where(x => test.Contains(x.Age));

            var result = ((ITypedSqlLinq)query).ToSQL();

            var fullQuery = result.ToQuery();
            //Assert.AreEqual("[Age] IN @sqlinq_1", result.Where);
            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age], [Is_Employed] AS [IsEmployed], [ParentID], [Column With Spaces] AS [ColumnWithSpaces] FROM [Person] WHERE [Age] IN @sqlinq_1", fullQuery);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);
        }
        #endregion

        #region Queryable 
        [TestMethod]
        public void Queryable_001()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            proxy.Where(x => x.IsEmployed == true);

            var result = (SQLinqSelectResult)person.ToSQL();

            Assert.AreEqual("[Is_Employed] = @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
        }

        [TestMethod]
        public void Queryable_002()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

             var final = (ITypedSqlLinq)proxy.Select(x => x).Where(x => x.IsEmployed == true).Where(x=>x.Age ==2).OrderBy(x=>x.FirstName);

            var result = (SQLinqSelectResult)final.ToSQL();

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[FirstName]", result.Select[1]);
            Assert.AreEqual("[Is_Employed] = @sqlinq_1 AND [Age] = @sqlinq_2", result.Where);
            Assert.AreEqual("[FirstName]", result.OrderBy[0]);
            Assert.AreEqual(2, result.Parameters.Count);
        }

        [TestMethod]
        public void Queryable_003()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            var result = proxy.Where(x => x.IsEmployed == true).Select(x => x.FirstName).ToList();

            Assert.IsNotNull(result);
        }


        [TestMethod]
        public void Queryable_004()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            var final = (ITypedSqlLinq)proxy.Where(x => x.IsEmployed == true).Where(x => x.Age == 2).OrderBy(x => x.FirstName).Select(x => new { x.FirstName, x.IsEmployed, x.Age });

            var result = (SQLinqSelectResult)final.ToSQL();

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[FirstName]", result.Select[0]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[1]);
            Assert.AreEqual("[Age]", result.Select[2]);
            Assert.AreEqual("[Is_Employed] = @sqlinq_1 AND [Age] = @sqlinq_2", result.Where);
            Assert.AreEqual(1, result.OrderBy.Length);
            Assert.AreEqual("[FirstName]", result.OrderBy[0]);
            Assert.AreEqual(2, result.Parameters.Count);
        }

        [TestMethod]
        public void Queryable_007()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            var final = (ITypedSqlLinq)proxy.Where(x => x.IsEmployed == true).Where(x => x.Age >= 2).OrderBy(x => x.FirstName).Select(x => new { x.FirstName, x.IsEmployed, x.Age });

            var result = (SQLinqSelectResult)final.ToSQL();

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[FirstName]", result.Select[0]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[1]);
            Assert.AreEqual("[Age]", result.Select[2]);
            Assert.AreEqual("[Is_Employed] = @sqlinq_1 AND [Age] >= @sqlinq_2", result.Where);
            Assert.AreEqual(1, result.OrderBy.Length);
            Assert.AreEqual("[FirstName]", result.OrderBy[0]);
            Assert.AreEqual(2, result.Parameters.Count);
        }
        [TestMethod]
        public void Queryable_007_Wrapped_Source_Obj()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            var proxyFilter = new Person() { Age = 2 };
            var final = (ITypedSqlLinq)proxy.Where(x => x.Age >= proxyFilter.Age).Select(x => x.ID);

            var result = (SQLinqSelectResult)final.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[Age] >= @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
        }

        [TestMethod]
        public void Queryable_007_Wrapped_Source_Obj_Nullable()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            var proxyFilter = new Child() { Height = 2};
            var final = (ITypedSqlLinq)proxy.Where(x => x.Age >= proxyFilter.Height).Select(x=>x.ID);

            var result = (SQLinqSelectResult)final.ToSQL();

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[ID]", result.Select[0]);
            Assert.AreEqual("[Age] >= @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
        }


        [TestMethod]
        public void Queryable_007_Wrapped_Source_Struct()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            var proxyFilter = new Range<int>() { Min = 2 };
            var final = (ITypedSqlLinq)proxy.Where(x => x.IsEmployed == true).Where(x => x.Age >= proxyFilter.Min).OrderBy(x => x.FirstName).Select(x => new { x.FirstName, x.IsEmployed, x.Age });

            var result = (SQLinqSelectResult)final.ToSQL();

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[FirstName]", result.Select[0]);
            Assert.AreEqual("[Is_Employed] AS [IsEmployed]", result.Select[1]);
            Assert.AreEqual("[Age]", result.Select[2]);
            Assert.AreEqual("[Is_Employed] = @sqlinq_1 AND [Age] >= @sqlinq_2", result.Where);
            Assert.AreEqual(1, result.OrderBy.Length);
            Assert.AreEqual("[FirstName]", result.OrderBy[0]);
            Assert.AreEqual(2, result.Parameters.Count);
        }

        [TestMethod]
        public void Queryable_005()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            var final = (ITypedSqlLinq) proxy.WithName("Test");

            var result = (SQLinqSelectResult)final.ToSQL();

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
        }

        [TestMethod]
        public void Queryable_006()
        {
            IQueryable<Person> people = new SQLinq<Person>();
            IQueryable<ParentPerson> parents = new SQLinq<ParentPerson>();
            //ITypedSqlLinq query = (ITypedSqlLinq)from p in people
            //            join parent in parents on p.ParentID equals parent.ID
            //            select new
            //            {
            //                Id = p.ID,
            //                FirstName = p.FirstName,
            //                LastName = p.LastName,
            //                ParentFirstName = parent.FirstName,
            //                ParentLastName = parent.LastName
            //            };
            ITypedSqlLinq query = (ITypedSqlLinq) people.Join(parents, p => p.ParentID,
                parent => parent.ID,
                (p, parent) => new {p.ID, p.FirstName, p.LastName, ParentFirstName = parent.FirstName, ParentLastName = parent.LastName})
                                                        .Select(p => new {p.ID, p.FirstName, p.LastName, p.ParentFirstName, p.ParentLastName})
                                                        .OrderBy(x => x.FirstName);
                                             
            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person] AS [p]", result.Table);

            Assert.AreEqual(1, result.Join.Length);
            Assert.AreEqual("JOIN [ParentPerson] AS [parent] ON [p].[ParentID] = [parent].[ID]", result.Join[0]);

            Assert.AreEqual(5, result.Select.Length);
            Assert.AreEqual("[p].[ID] AS [ID]", result.Select[0]);
            Assert.AreEqual("[p].[FirstName] AS [FirstName]", result.Select[1]);
            Assert.AreEqual("[p].[LastName] AS [LastName]", result.Select[2]);
            Assert.AreEqual("[parent].[FirstName] AS [ParentFirstName]", result.Select[3]);
            Assert.AreEqual("[parent].[LastName] AS [ParentLastName]", result.Select[4]);


            Assert.AreEqual(1, result.OrderBy.Length);
            Assert.AreEqual("[p].[FirstName]", result.OrderBy[1]);
        }

        [TestMethod]
        public void Pluggable_Method_Call_Handlers_001()
        {
            var person = new SQLinq<Person>();

            var proxy = ProxyFilters.Apply(person);

            var final = (ITypedSqlLinq)proxy.WithName("Test");

            var result = (SQLinqSelectResult)final.ToSQL();

            Assert.AreEqual(7, result.Select.Length);
            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);
        }


        #endregion

        #region WHERE using Field

        [TestMethod]
        public void WHERE_usingField_001()
        {
            var query = from d in new SQLinq<WHERE_usingField_001_Object>()
                        where d.ID == 32
                        select d;

            var actual = query.ToSQL();

            var actualQuery = actual.ToQuery();
            var expectedQuery = @"SELECT * FROM [WHERE_usingField_001_Object] WHERE [ID] = @sqlinq_1";

            Assert.AreEqual(expectedQuery, actualQuery);

            Assert.AreEqual(1, actual.Parameters.Count);
            Assert.AreEqual(32, actual.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void WHERE_usingField_002()
        {
            var query = from d in new SQLinq<WHERE_usingField_001_Object>()
                        where d.ID == 32
                        select d.Name;

            var actual = query.ToSQL();

            var actualQuery = actual.ToQuery();
            var expectedQuery = @"SELECT [Name] FROM [WHERE_usingField_001_Object] WHERE [ID] = @sqlinq_1";

            Assert.AreEqual(expectedQuery, actualQuery);

            Assert.AreEqual(1, actual.Parameters.Count);
            Assert.AreEqual(32, actual.Parameters["@sqlinq_1"]);
        }

        public class WHERE_usingField_001_Object
        {
            public int ID;
            public string Name;
        }

        #endregion

        #region Group By

        [TestMethod]
        public void SQLinqTest_Group_By_001()
        {

            var query = from p in new SQLinq<Car>()
                        group p by p.Make into g
                        select new
                        {
                            Id = g.Key
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make] AS [Id] FROM [Car] GROUP BY [Make]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(1, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[Make] AS [Id]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_001_Queryable()
        {
            IQueryable<Car> start = new SQLinq<Car>();
            ITypedSqlLinq query = (ITypedSqlLinq)from p in start
                                                    group p by p.Make into g
                                                    select new
                                                    {
                                                        Id = g.Key
                                                    };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make] AS [Id] FROM [Car] GROUP BY [Make]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(1, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[Make] AS [Id]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_002()
        {
            var query = from p in new SQLinq<Car>()
                        where p.Make == "Ford"
                        group p by p.Make into g
                        orderby g.Key descending 
                        select new
                        {
                            Id = g.Key
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make] AS [Id] FROM [Car] WHERE [Make] = @sqlinq_1 GROUP BY [Make] ORDER BY [Make] DESC", result.ToQuery());
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual("Ford", result.Parameters["@sqlinq_1"]);

            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(1, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(1, result.Select.Length);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_003()
        {

            var query = from p in new SQLinq<Car>()
                        group p by new { p.Make, p.ParentId } into g
                        select new
                        {
                            Make = g.Key.Make,
                            ParentId = g.Key.ParentId
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make], [ParentId] FROM [Car] GROUP BY [Make], [ParentId]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_003_Queryable()
        {
            IQueryable<Car> cars = new SQLinq<Car>();
            ITypedSqlLinq query = (ITypedSqlLinq)from p in cars
                                                 group p by new { p.Make, p.ParentId } into g
                                                 select new
                                                 {
                                                     Make = g.Key.Make,
                                                     ParentId = g.Key.ParentId
                                                 };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make], [ParentId] FROM [Car] GROUP BY [Make], [ParentId]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_004()
        {
            var query = from p in new SQLinq<Car>()
                        join c in new SQLinq<Person>() on p.ParentId equals c.ID
                        group p by p.Make into g
                        select new
                        {
                            Id = g.Key
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [p].[Make] AS [Id] FROM [Car] AS [p] JOIN [Person] AS [c] ON [p].[ParentId] = [c].[ID] GROUP BY [p].[Make]", result.ToQuery());
            Assert.AreEqual("[Car] AS [p]", result.Table);
            Assert.AreEqual(1, result.GroupBy.Length);
            Assert.AreEqual("[p].[Make]", result.GroupBy[0]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[p].[Make] AS [Id]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_004_Queryable()
        {
            IQueryable<Car> cars = new SQLinq<Car>();
            IQueryable<Person> people = new SQLinq<Person>();

            var query = from p in cars
                        join c in people on p.ParentId equals c.ID
                        group p by p.Make into g
                        select new
                        {
                            Id = g.Key
                        };

            var result = (SQLinqSelectResult)((ITypedSqlLinq)query).ToSQL();

            Assert.AreEqual("SELECT [p].[Make] AS [Id] FROM [Car] AS [p] JOIN [Person] AS [c] ON [p].[ParentId] = [c].[ID] GROUP BY [p].[Make]", result.ToQuery());
            Assert.AreEqual("[Car] AS [p]", result.Table);
            Assert.AreEqual(1, result.GroupBy.Length);
            Assert.AreEqual("[p].[Make]", result.GroupBy[0]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("[p].[Make] AS [Id]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_005()
        {

            var query = from p in new SQLinq<Car>()
                        group p by new { Test = p.Make, Test2 = p.ParentId } into g
                        select new
                        {
                            Make = g.Key.Test,
                            ParentId = g.Key.Test2
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make], [ParentId] FROM [Car] GROUP BY [Make], [ParentId]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_005_Queryable()
        {
            IQueryable<Car> cars = new SQLinq<Car>();
            ITypedSqlLinq query = (ITypedSqlLinq)from p in cars
                                                 group p by new { Test = p.Make, Test2 = p.ParentId } into g
                                                 select new
                                                 {
                                                     Make = g.Key.Test,
                                                     ParentId = g.Key.Test2
                                                 };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make], [ParentId] FROM [Car] GROUP BY [Make], [ParentId]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_005_Join()
        {

            var query = from p in new SQLinq<Car>()
                        join c in new SQLinq<Person>() on p.ParentId equals c.ID
                        group p by new { Test = p.Make, Test2 = p.ParentId } into g
                        select new
                        {
                            Make = g.Key.Test,
                            ParentId = g.Key.Test2
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [p].[Make] AS [Make], [p].[ParentId] AS [ParentId] FROM [Car] AS [p] JOIN [Person] AS [c] ON [p].[ParentId] = [c].[ID] GROUP BY [p].[Make], [p].[ParentId]", result.ToQuery());
            Assert.AreEqual("[Car] AS [p]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[p].[Make]", result.GroupBy[0]);

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[p].[Make] AS [Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_005_Queryable_Join()
        {
            IQueryable<Car> cars = new SQLinq<Car>();
            IQueryable<Person> people = new SQLinq<Person>();
            var query = from p in cars
                        join c in people on p.ParentId equals c.ID
                        group p by new { Test = p.Make, Test2 = p.ParentId } into g
                                                 select new
                                                 {
                                                     Make = g.Key.Test,
                                                     ParentId = g.Key.Test2
                                                 };

            var result = (SQLinqSelectResult)((ITypedSqlLinq)query).ToSQL();

            Assert.AreEqual("SELECT [p].[Make] AS [Make], [p].[ParentId] AS [ParentId] FROM [Car] AS [p] JOIN [Person] AS [c] ON [p].[ParentId] = [c].[ID] GROUP BY [p].[Make], [p].[ParentId]", result.ToQuery());
            Assert.AreEqual("[Car] AS [p]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[p].[Make]", result.GroupBy[0]);

            Assert.AreEqual(2, result.Select.Length);
            Assert.AreEqual("[p].[Make] AS [Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_006()
        {

            var query = from p in new SQLinq<Car>()
                        group p by new { p.Make, p.ParentId } into g
                        select new
                        {
                            Make = g.Key.Make,
                            ParentId = g.Key.ParentId,
                            Count = g.Count()
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make], [ParentId], Count(*) AS [Count] FROM [Car] GROUP BY [Make], [ParentId]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_006_Queryable()
        {
            IQueryable<Car> cars = new SQLinq<Car>();
            ITypedSqlLinq query = (ITypedSqlLinq)from p in cars
                                                 group p by new { p.Make, p.ParentId } into g
                                                 select new
                                                 {
                                                     Make = g.Key.Make,
                                                     ParentId = g.Key.ParentId,
                                                     Count = g.Count()
                                                 };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make], [ParentId], Count(*) AS [Count] FROM [Car] GROUP BY [Make], [ParentId]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[Make]", result.Select[0]);
        }


        [TestMethod]
        public void SQLinqTest_Group_By_007()
        {

            var query = from p in new SQLinq<Car>()
                        group p by new { Test = p.Make, Test2 = p.ParentId } into g
                        select new
                        {
                            Make = g.Key.Test,
                            ParentId = g.Key.Test2,
                            Count = g.Count()
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make], [ParentId], Count(*) AS [Count] FROM [Car] GROUP BY [Make], [ParentId]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_007_Queryable()
        {
            IQueryable<Car> cars = new SQLinq<Car>();
            ITypedSqlLinq query = (ITypedSqlLinq)from p in cars
                                                 group p by new { Test = p.Make, Test2 = p.ParentId } into g
                                                 select new
                                                 {
                                                     Make = g.Key.Test,
                                                     ParentId = g.Key.Test2,
                                                     Count = g.Count()
                                                 };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [Make], [ParentId], Count(*) AS [Count] FROM [Car] GROUP BY [Make], [ParentId]", result.ToQuery());
            Assert.AreEqual("[Car]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[Make]", result.GroupBy[0]);

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_007_Join()
        {

            var query = from p in new SQLinq<Car>()
                        join c in new SQLinq<Person>() on p.ParentId equals c.ID
                        group p by new { Test = p.Make, Test2 = p.ParentId } into g
                        select new
                        {
                            Make = g.Key.Test,
                            ParentId = g.Key.Test2,
                            Count = g.Count()
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("SELECT [p].[Make] AS [Make], [p].[ParentId] AS [ParentId], Count(*) AS [Count] FROM [Car] AS [p] JOIN [Person] AS [c] ON [p].[ParentId] = [c].[ID] GROUP BY [p].[Make], [p].[ParentId]", result.ToQuery());
            Assert.AreEqual("[Car] AS [p]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[p].[Make]", result.GroupBy[0]);

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[p].[Make] AS [Make]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_007_Queryable_Join()
        {
            IQueryable<Car> cars = new SQLinq<Car>();
            IQueryable<Person> people = new SQLinq<Person>();
            var query = from p in cars
                        join c in people on p.ParentId equals c.ID
                        group p by new { Test = p.Make, Test2 = p.ParentId } into g
                        select new
                        {
                            Random = g.Key.Test,
                            Random2 = g.Key.Test2,
                            Count = g.Count()
                        };

            var result = (SQLinqSelectResult)((ITypedSqlLinq)query).ToSQL();

            Assert.AreEqual("SELECT [p].[Make] AS [Random], [p].[ParentId] AS [Random2], Count(*) AS [Count] FROM [Car] AS [p] JOIN [Person] AS [c] ON [p].[ParentId] = [c].[ID] GROUP BY [p].[Make], [p].[ParentId]", result.ToQuery());
            Assert.AreEqual("[Car] AS [p]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[p].[Make]", result.GroupBy[0]);

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[p].[Make] AS [Random]", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Group_By_008_Queryable_Join()
        {
            IQueryable<Car> cars = new SQLinq<Car>();
            IQueryable<Person> people = new SQLinq<Person>();
            IQueryable<ParentPerson> parents = new SQLinq<ParentPerson>();
            var query = from p in cars
                        join c in people on p.ParentId equals c.ID
                        join pp in parents on p.ParentId equals pp.ID
                        where pp.IsEmployed
                        orderby pp.Age
                        group p by new { Test = p.Make, p.ParentId } into g
                        select g;

            var result = (SQLinqSelectResult)((ITypedSqlLinq)query).ToSQL();

            Assert.AreEqual("SELECT [p].[Make] AS [Test], [p].[ParentId] AS [ParentId] FROM [Car] AS [p] JOIN [ParentPerson] AS [pp] ON [p].[ParentId] = [pp].[ID] JOIN [Person] AS [c] ON [p].[ParentId] = [c].[ID] WHERE NULL GROUP BY [p].[Make], [p].[ParentId] ORDER BY [pp].[Age]", result.ToQuery());
            Assert.AreEqual("[Car] AS [p]", result.Table);
            Assert.AreEqual(2, result.GroupBy.Length);
            Assert.AreEqual("[p].[Make]", result.GroupBy[0]);

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[p].[Make] AS [Random]", result.Select[0]);
        }

        #endregion

        #region Table Joins

        [TestMethod]
        public void SQLinqTest_Join_001()
        {
            var query = from p in new SQLinq<Person>()
                        join parent in new SQLinq<ParentPerson>() on p.ParentID equals parent.ID
                        select new
                        {
                            Id = p.ID,
                            FirstName = p.FirstName,
                            LastName = p.LastName,
                            ParentFirstName = parent.FirstName,
                            ParentLastName = parent.LastName
                        };

            var result = (SQLinqSelectResult) query.ToSQL();

            Assert.AreEqual("[Person] AS [p]", result.Table);

            Assert.AreEqual(1, result.Join.Length);
            Assert.AreEqual("JOIN [ParentPerson] AS [parent] ON [p].[ParentID] = [parent].[ID]", result.Join[0]);

            Assert.AreEqual(5, result.Select.Length);
            Assert.AreEqual("[p].[ID] AS [Id]", result.Select[0]);
            Assert.AreEqual("[p].[FirstName] AS [FirstName]", result.Select[1]);
            Assert.AreEqual("[p].[LastName] AS [LastName]", result.Select[2]);
            Assert.AreEqual("[parent].[FirstName] AS [ParentFirstName]", result.Select[3]);
            Assert.AreEqual("[parent].[LastName] AS [ParentLastName]", result.Select[4]);
        }

        [TestMethod]
        public void SQLinqTest_Join_002()
        {
            //Example of what we want
            //var people2 = new List<Person>();
            //var parents2 = new List<ParentPerson>();
            //var cars2 = new List<Car>();

            //var query2 = from p in people2
            //             join parent in parents2 on p.ParentID equals parent.ID
            //             join car in cars2 on p.ID equals car.ParentId
            //             select new
            //             {
            //                 Id = p.ID,
            //                 FirstName = p.FirstName,
            //                 LastName = p.LastName,
            //                 ParentFirstName = parent.FirstName,
            //                 ParentLastName = parent.LastName,
            //                 CarName = car.Name
            //             };

            var people = new SQLinq<Person>();
            var parents = new SQLinq<ParentPerson>();
            var cars = new SQLinq<Car>();

            var query = from p in people
                        join pt in parents on p.ParentID equals pt.ID 
                        join c in cars on p.ID equals c.ParentId
                        select new
                        {
                            Id = p.ID,
                            FirstName = p.FirstName,
                            LastName = p.LastName,
                            ParentFirstName = pt.FirstName,
                            ParentLastName = pt.LastName,
                            CarName = c.Name
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person] AS [p]", result.Table);

            Assert.AreEqual(2, result.Join.Length);
            Assert.AreEqual("JOIN [ParentPerson] AS [pt] ON [p].[ParentID] = [pt].[ID]", result.Join[0]);
            Assert.AreEqual("JOIN [Car] AS [c] ON [p].[ID] = [c].[ParentId]", result.Join[1]);

            Assert.AreEqual(6, result.Select.Length);
            Assert.AreEqual("[p].[ID] AS [Id]", result.Select[0]);
            Assert.AreEqual("[p].[FirstName] AS [FirstName]", result.Select[1]);
            Assert.AreEqual("[p].[LastName] AS [LastName]", result.Select[2]);
            Assert.AreEqual("[pt].[FirstName] AS [ParentFirstName]", result.Select[3]);
            Assert.AreEqual("[pt].[LastName] AS [ParentLastName]", result.Select[4]);
            Assert.AreEqual("[c].[Name] AS [CarName]", result.Select[5]);
        }

        [TestMethod]
        public void SQLinqTest_Join_003()
        {
            var people = new SQLinq<Person>();
            var parents = new SQLinq<ParentPerson>();

            var query = from p in people
                        join pt in parents on p.ParentID equals pt.ID
                        where p.FirstName.StartsWith("SomeCar") && pt.IsEmployed == true
                        select new
                        {
                            Id = p.ID,
                            FirstName = p.FirstName,
                            LastName = p.LastName,
                            ParentFirstName = pt.FirstName,
                            ParentLastName = pt.LastName
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person] AS [p]", result.Table);

            Assert.AreEqual(1, result.Join.Length);
            Assert.AreEqual("JOIN [ParentPerson] AS [pt] ON [p].[ParentID] = [pt].[ID]", result.Join[0]);

            Assert.AreEqual("([p].[FirstName] LIKE @sqlinq_1 AND [pt].[Is_Employed] = @sqlinq_2)", result.Where);

            Assert.AreEqual(5, result.Select.Length);
            Assert.AreEqual("[p].[ID] AS [Id]", result.Select[0]);
            Assert.AreEqual("[p].[FirstName] AS [FirstName]", result.Select[1]);
            Assert.AreEqual("[p].[LastName] AS [LastName]", result.Select[2]);
            Assert.AreEqual("[pt].[FirstName] AS [ParentFirstName]", result.Select[3]);
            Assert.AreEqual("[pt].[LastName] AS [ParentLastName]", result.Select[4]);
        }

        [TestMethod]
        public void SQLinqTest_Join_004()
        {
            var people = new SQLinq<Person>();
            var parents = new SQLinq<ParentPerson>();
            var cars = new SQLinq<Car>();

            var query = from p in people
                        join pt in parents on p.ParentID equals pt.ID
                        join c in cars on p.ID equals c.ParentId
                        where p.FirstName.StartsWith("John") && pt.IsEmployed == true && c.Name.EndsWith("Honda")
                        select new
                        {
                            Id = p.ID,
                            FirstName = p.FirstName,
                            LastName = p.LastName,
                            ParentFirstName = pt.FirstName,
                            ParentLastName = pt.LastName,
                            CarName = c.Name
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person] AS [p]", result.Table);

            Assert.AreEqual(2, result.Join.Length);
            Assert.AreEqual("JOIN [ParentPerson] AS [pt] ON [p].[ParentID] = [pt].[ID]", result.Join[0]);
            Assert.AreEqual("JOIN [Car] AS [c] ON [p].[ID] = [c].[ParentId]", result.Join[1]);

            Assert.AreEqual("(([p].[FirstName] LIKE @sqlinq_1 AND [pt].[Is_Employed] = @sqlinq_2) AND [c].[Name] LIKE @sqlinq_3)", result.Where);

            Assert.AreEqual(6, result.Select.Length);
            Assert.AreEqual("[p].[ID] AS [Id]", result.Select[0]);
            Assert.AreEqual("[p].[FirstName] AS [FirstName]", result.Select[1]);
            Assert.AreEqual("[p].[LastName] AS [LastName]", result.Select[2]);
            Assert.AreEqual("[pt].[FirstName] AS [ParentFirstName]", result.Select[3]);
            Assert.AreEqual("[pt].[LastName] AS [ParentLastName]", result.Select[4]);
            Assert.AreEqual("[c].[Name] AS [CarName]", result.Select[5]);

            var finalSql = result.ToQuery();
        }

        [TestMethod]
        public void SQLinqTest_Join_005()
        {
            var people = new SQLinq<Person>();
            var items = new SQLinq<GenericTypeTestClass>();

            var query = from p in people
                        join c in items on p.Age equals c.Price.Value
                        where p.FirstName.StartsWith("John") && c.Name.EndsWith("Honda")
                        select new
                        {
                            Id = p.ID,
                            FirstName = p.FirstName,
                            LastName = p.LastName,
                            CarName = c.Name
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person] AS [p]", result.Table);

            Assert.AreEqual(1, result.Join.Length);
            Assert.AreEqual("JOIN [GenericTypeTestClass] AS [c] ON [p].[Age] = [c].[Price]", result.Join[0]);

            Assert.AreEqual("([p].[FirstName] LIKE @sqlinq_1 AND [c].[Name] LIKE @sqlinq_2)", result.Where);

            Assert.AreEqual(4, result.Select.Length);
            Assert.AreEqual("[p].[ID] AS [Id]", result.Select[0]);
            Assert.AreEqual("[p].[FirstName] AS [FirstName]", result.Select[1]);
            Assert.AreEqual("[p].[LastName] AS [LastName]", result.Select[2]);
            Assert.AreEqual("[c].[Name] AS [CarName]", result.Select[3]);

            var finalSql = result.ToQuery();
        }

        [TestMethod]
        public void SQLinqTest_Join_006()
        {
            var people = new SQLinq<Person>();
            var items = new SQLinq<GenericTypeTestClass>();

            var query = from p in people.Where(x => x.IsEmployed==true)
                        join c in items on p.Age equals c.Price.Value
                        where c.Price.HasValue
                        select new
                        {
                            Id = p.ID,
                            FirstName = p.FirstName,
                            LastName = p.LastName
                        };

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person] AS [p]", result.Table);

            Assert.AreEqual(1, result.Join.Length);
            Assert.AreEqual("JOIN [GenericTypeTestClass] AS [c] ON [p].[Age] = [c].[Price]", result.Join[0]);
            
            Assert.AreEqual("[p].[Is_Employed] = @sqlinq_1 AND [c].[Price] IS NOT NULL", result.Where);

            Assert.AreEqual(3, result.Select.Length);
            Assert.AreEqual("[p].[ID] AS [Id]", result.Select[0]);
            Assert.AreEqual("[p].[FirstName] AS [FirstName]", result.Select[1]);
            Assert.AreEqual("[p].[LastName] AS [LastName]", result.Select[2]);

            var finalSql = result.ToQuery();
        }

        #endregion

        #region Static Insert

        [TestMethod]
        public void Insert_001()
        {
            var actual = SQLinq.SQLinq.Insert(new Person(), "People");
            Assert.IsInstanceOfType(actual, typeof(SQLinqInsert<Person>));
            Assert.AreEqual("People", actual.TableNameOverride);
        }

        [TestMethod]
        public void Insert_002()
        {
            var actual = SQLinq.SQLinq.Insert(new Person());
            Assert.IsInstanceOfType(actual, typeof(SQLinqInsert<Person>));
            Assert.AreEqual(null, actual.TableNameOverride);
        }

        [TestMethod]
        public void Insert_003()
        {
            var dict = new  Dictionary<string, object>();
            var actual = new DynamicSQLinqInsert(dict, "MyTable");
            Assert.IsInstanceOfType(actual, typeof(DynamicSQLinqInsert));
            Assert.AreEqual("MyTable", actual.Table);
        }

        [TestMethod]
        public void Update_001()
        {
            var actual = SQLinq.SQLinq.Update(new Person(), "People");
            Assert.IsInstanceOfType(actual, typeof(SQLinqUpdate<Person>));
            Assert.AreEqual("People", actual.TableNameOverride);
        }

        [TestMethod]
        public void Update_002()
        {
            var actual = SQLinq.SQLinq.Update(new Person());
            Assert.IsInstanceOfType(actual, typeof(SQLinqUpdate<Person>));
            Assert.AreEqual(null, actual.TableNameOverride);
        }

        [TestMethod]
        public void Update_003()
        {
            var dict = new Dictionary<string, object>();
            var actual = new DynamicSQLinqUpdate(dict, "MyTable");
            Assert.IsInstanceOfType(actual, typeof(DynamicSQLinqUpdate));
            Assert.AreEqual("MyTable", actual.Table);
        }

        #endregion

        #region SELECT COUNT

        [TestMethod]
        public void SQLinqTest_Count_001()
        {
            var query = (from d in new SQLinq<Person>()
                        where d.Age == 12
                        select d).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] = @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_002()
        {
            var query = new SQLinq<Person>().Where(d => d.FirstName == "Chris").Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[FirstName] = @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual("Chris", result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void SQLinqTest_Count_003()
        {
            var query = new SQLinq<ICar>().Where(d => d.WheelDiameter == 15).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[WheelDiameter] = @sqlinq_1", result.Where);
            Assert.AreEqual(15, result.Parameters["@sqlinq_1"]);
        }

        [TestMethod]
        public void SQLinqTest_Count_004()
        {
            var query = (from d in new SQLinq<Person>("PersonTableOverride")
                        where d.Age == 12
                        select d).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[PersonTableOverride]", result.Table);

            Assert.AreEqual("[Age] = @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_005()
        {
            var obj = new Person();

            var query = (from d in obj.ToSQLinq()
                        where d.Age == 12 && d.FirstName.StartsWith("Ar")
                        select d).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("([Age] = @sqlinq_1 AND [FirstName] LIKE @sqlinq_2)", result.Where);

            Assert.AreEqual(2, result.Parameters.Count);
            Assert.AreEqual(12, result.Parameters["@sqlinq_1"]);
            Assert.AreEqual("Ar%", result.Parameters["@sqlinq_2"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_006()
        {
            var query = (from d in new SQLinq<Person>()
                        where d.Age < 104
                        select new
                        {
                            FirstName = d.FirstName.ToUpper()
                        }).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(104, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_007()
        {
            var query = (from d in new SQLinq<Person>()
                        where d.Age < 104
                        select d.FirstName.ToUpper()).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(104, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_008()
        {
            var query = (from d in new SQLinq<Person>()
                        where d.Age < 104
                        select d.FirstName).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(104, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_009()
        {
            var query = (from d in new SQLinq<Person>()
                        where d.Age < 104
                        select d.FirstName.Length).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[Age] < @sqlinq_1", result.Where);

            Assert.AreEqual(1, result.Parameters.Count);
            Assert.AreEqual(104, result.Parameters["@sqlinq_1"]);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_010()
        {
            var query = (from d in new SQLinq<Person>()
                        where d.FirstName == null
                        select d).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[FirstName] IS NULL", result.Where);

            Assert.AreEqual(0, result.Parameters.Count);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_011()
        {
            var query = (from d in new SQLinq<Person>()
                        where d.FirstName != null
                        select d).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[FirstName] IS NOT NULL", result.Where);

            Assert.AreEqual(0, result.Parameters.Count);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        [TestMethod]
        public void SQLinqTest_Count_012()
        {
            string nullVal = null;
            var query = (from d in new SQLinq<Person>()
                        where d.FirstName != nullVal
                        select d).Count();

            var result = (SQLinqSelectResult)query.ToSQL();

            Assert.AreEqual("[Person]", result.Table);

            Assert.AreEqual("[FirstName] IS NOT NULL", result.Where);

            Assert.AreEqual(0, result.Parameters.Count);

            Assert.AreEqual(1, result.Select.Length);
            Assert.AreEqual("COUNT(1)", result.Select[0]);
        }

        #endregion
    }

    
    public struct Range<T> where T : struct
    {
        public Range(T? min, T? max) : this()
        {
            Min = min;
            Max = max;
        }
        
        public T? Min { get; set; }

        public T? Max { get; set; }

        public bool HasValues()
        {
            return Min.HasValue || Max.HasValue;
        }
    }

}