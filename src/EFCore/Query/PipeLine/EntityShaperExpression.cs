// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Microsoft.EntityFrameworkCore.Query.Pipeline
{
    public class EntityShaperExpression : Expression
    {
        public EntityShaperExpression(IEntityType entityType, ProjectionBindingExpression valueBufferExpression)
        {
            EntityType = entityType;
            ValueBufferExpression = valueBufferExpression;
        }

        public IEntityType EntityType { get; }
        public ProjectionBindingExpression ValueBufferExpression { get; }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var valueBufferExpression = (ProjectionBindingExpression)visitor.Visit(ValueBufferExpression);

            return Update(valueBufferExpression);
        }

        public EntityShaperExpression Update(ProjectionBindingExpression valueBufferExpression)
        {
            return valueBufferExpression != ValueBufferExpression
                ? new EntityShaperExpression(EntityType, valueBufferExpression)
                : this;
        }

        public override Type Type => EntityType.ClrType;
        public override ExpressionType NodeType => ExpressionType.Extension;
    }
}
