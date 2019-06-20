//Copyright (c) Chris Pietschmann 2012 (http://pietschsoft.com)
//Licensed under the GNU Library General Public License (LGPL)
//License can be found here: http://sqlinq.codeplex.com/license

using System;
using SQLinq;

namespace SQLinqTest
{
    public class Child
    {
        public int? Height { get; set; }
        public Guid? ParentID { get; set; }
    }
    public class Person
    {
        public Guid ID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int Age { get; set; }

        [SQLinqColumn("[Is_Employed]")]
        public bool IsEmployed { get; set; }

        public Guid ParentID { get; set; }

        [SQLinqColumn("Column With Spaces")]
        public string ColumnWithSpaces { get; set; }
    }

    public class PersonFirstNameNoSelect
    {
        public Guid ID { get; set; }
        [SQLinqColumn(select:false)]
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int Age { get; set; }

        [SQLinqColumn("[Is_Employed]")]
        public bool IsEmployed { get; set; }

        public Guid ParentID { get; set; }

        [SQLinqColumn("Column With Spaces")]
        public string ColumnWithSpaces { get; set; }
    }
}
