// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.Expressions.Internal
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
            expressionPrinter.StringBuilder.Append("BINDING(");
            expressionPrinter.Visit(RootParameter);
            expressionPrinter.StringBuilder.Append(" | ");

            // TODO: fix this
            expressionPrinter.StringBuilder.Append(string.Join(", ", NavigationTreeNode.FromMappings.First()) + ")");
        }
    }

    public class CustomRootExpression : Expression, IPrintable
    {
        public ParameterExpression RootParameter { get; }
        public List<string> Mapping { get; }
        public override ExpressionType NodeType => ExpressionType.Extension;
        public override bool CanReduce => false;
        public override Type Type { get; }

        public CustomRootExpression(ParameterExpression rootParameter, List<string> mapping, Type type)
        {
            RootParameter = rootParameter;
            Mapping = mapping;
            Type = type;
        }

        public void Print([NotNull] ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.StringBuilder.Append("CUSTOM_ROOT(");
            expressionPrinter.Visit(RootParameter);
            expressionPrinter.StringBuilder.Append(" | ");

            expressionPrinter.StringBuilder.Append(string.Join(", ", Mapping) + ")");
        }
    }

    public class CustomRootExpression2 : Expression, IPrintable
    {
        public Expression Root { get; }
        public List<string> Mapping { get; }
        public override ExpressionType NodeType => ExpressionType.Extension;
        public override bool CanReduce => false;
        public override Type Type { get; }

        public CustomRootExpression2(Expression root, List<string> mapping, Type type)
        {
            Root = root;
            Mapping = mapping;
            Type = type;
        }

        // TODO: HACK!!!
        public Expression Unwrap()
        {
            if (Mapping.Count == 0)
            {
                return Root;
            }

            if (Root is NavigationExpansionExpression navigationExpansionExpression)
            {
                var newOperand = navigationExpansionExpression.Operand;
                foreach (var mappingElement in Mapping)
                {
                    newOperand = PropertyOrField(newOperand, mappingElement);
                }

                return new NavigationExpansionExpression(newOperand, navigationExpansionExpression.State, navigationExpansionExpression.Type);
            }

            var result = Root;
            foreach (var mappingElement in Mapping)
            {
                result = PropertyOrField(result, mappingElement);
            }

            return result;
        }

        public void Print([NotNull] ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.StringBuilder.Append("CUSTOM_ROOT(");
            expressionPrinter.Visit(Root);
            expressionPrinter.StringBuilder.Append(" | ");

            expressionPrinter.StringBuilder.Append(string.Join(", ", Mapping) + ")");
        }
    }
}
