using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.Server;

namespace SqlServerAggregateFunctions
{
    [Serializable]
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = -1)]
    public struct Concat : IBinarySerialize
    {
        public List<string> list;

        public void Init()
        {
            list = new List<string>();
        }

        public void Accumulate(SqlString value, SqlString delim)
        {
            list = list.Concat(new List<string> {value.Value})
                .Concat(new List<string> {delim.IsNull ? "" : delim.Value})
                .ToList();
        }

        public void Merge(Concat value)
        {
            list = list.Concat(value.list)
                .ToList();
        }

        public SqlString Terminate()
        {
            // Trim off the beginning and ending comma's
            return new SqlString(list.Take(list.Count - 1).Aggregate(string.Concat));
        }

        public void Read(BinaryReader r)
        {
            list = new List<string>();
            int tokenCount = r.ReadInt32();
            while (tokenCount-- > 0)
                list = list.Concat(new List<string> {r.ReadString()})
                    .ToList();
        }

        public void Write(BinaryWriter w)
        {
            w.Write(list.Count);
            foreach (var token in list)
                w.Write(token);
        }
    }

}