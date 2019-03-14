// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion
{
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
            expressionPrinter.StringBuilder.Append("CUSTOM_ROOT([" + Type.ShortDisplayName() + "] | ");
            expressionPrinter.Visit(RootParameter);
            expressionPrinter.StringBuilder.Append(".");
            expressionPrinter.StringBuilder.Append(string.Join(".", Mapping) + ")");
        }
    }
}
