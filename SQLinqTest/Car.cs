using System;

namespace SQLinqTest
{
    public class Car
    {
        public Guid ID { get; set; }
        public string Name { get; set; }
        public string Make { get; set; }

        public Guid ParentId { get; set; }
    }
}