// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion
{
    public class NavigationBindingExpression : Expression, IPrintable
    {
        public ParameterExpression RootParameter { get; }
        public IEntityType EntityType { get; }
        public NavigationTreeNode NavigationTreeNode { get; }
        public SourceMapping SourceMapping { get; }

        public override ExpressionType NodeType => ExpressionType.Extension;
        public override bool CanReduce => false;
        public override Type Type { get; }

        public NavigationBindingExpression(
            ParameterExpression rootParameter,
            NavigationTreeNode navigationTreeNode,
            IEntityType entityType,
            SourceMapping sourceMapping,
            Type type)
        {
            RootParameter = rootParameter;
            NavigationTreeNode = navigationTreeNode;
            EntityType = entityType;
            SourceMapping = sourceMapping;
            Type = type;
        }

        public void Print([NotNull] ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.StringBuilder.Append("BINDING([" + EntityType.ClrType.ShortDisplayName() + "] | ");
            expressionPrinter.StringBuilder.Append(string.Join(".", NavigationTreeNode.FromMappings.First()) + " -> ");
            expressionPrinter.Visit(RootParameter);
            expressionPrinter.StringBuilder.Append(".");
            expressionPrinter.StringBuilder.Append(string.Join(".", NavigationTreeNode.ToMapping) + ")");
        }
    }
}
