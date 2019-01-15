// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace Microsoft.EntityFrameworkCore.Relational.Query.Pipeline.SqlExpressions
{
    public class ColumnExpression : SqlExpression
    {
        #region Fields & Constructors

        internal ColumnExpression(IProperty property, TableExpressionBase table)
            : this(property.Relational().ColumnName, table, property.ClrType, property.FindRelationalMapping())
        {
        }

        internal ColumnExpression(ProjectionExpression subqueryProjection, TableExpressionBase table)
            : this(subqueryProjection.Alias, table, subqueryProjection.Type, subqueryProjection.Expression.TypeMapping)
        {
        }

        private ColumnExpression(string name, TableExpressionBase table, Type type, RelationalTypeMapping typeMapping)
            : base(type, typeMapping)
        {
            Name = name;
            Table = table;
        }

        #endregion

        #region Public Properties

        public string Name { get; }
        public TableExpressionBase Table { get; }

        #endregion

        #region Expression-based methods

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var newTable = (TableExpressionBase)visitor.Visit(Table);

            return newTable != Table
                ? new ColumnExpression(Name, newTable, Type, TypeMapping)
                : this;
        }

        #endregion

        #region Equality & HashCode

        public override bool Equals(object obj)
            => obj != null
            && (ReferenceEquals(this, obj)
                || obj is ColumnExpression columnExpression
                    && Equals(columnExpression));

        private bool Equals(ColumnExpression columnExpression)
            => base.Equals(columnExpression)
            && string.Equals(Name, columnExpression.Name)
            && Table.Equals(columnExpression.Table);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ Name.GetHashCode();
                hashCode = (hashCode * 397) ^ Table.GetHashCode();

                return hashCode;
            }
        }

        #endregion
    }
}
