// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion
{
    public class NavigationExpansionExpressionState
    {
        public NavigationExpansionExpressionState(ParameterExpression currentParameter)
            : this(currentParameter, new List<SourceMapping>(), null, false, null, new List<List<string>>()/*, new List<NestedExpansionMapping>()*/)
        {
        }

        public NavigationExpansionExpressionState(
            ParameterExpression currentParameter,
            List<SourceMapping> sourceMappings,
            LambdaExpression pendingSelector,
            bool applyPendingSelector,
            MethodInfo pendingTerminatingOperator,
            List<List<string>> customRootMappings/*,
            List<NestedExpansionMapping> nestedExpansionMappings*/)
        {
            CurrentParameter = currentParameter;
            SourceMappings = sourceMappings;
            PendingSelector = pendingSelector;
            ApplyPendingSelector = applyPendingSelector;
            PendingTerminatingOperator = pendingTerminatingOperator;
            CustomRootMappings = customRootMappings;
            //NestedExpansionMappings = nestedExpansionMappings;
        }

        public ParameterExpression CurrentParameter { get; set; }
        public List<SourceMapping> SourceMappings { get; set; }
        public LambdaExpression PendingSelector { get; set; }
        public MethodInfo PendingTerminatingOperator { get; set; }
        public bool ApplyPendingSelector { get; set; }
        public List<List<string>> CustomRootMappings { get; set; }
        //public List<NestedExpansionMapping> NestedExpansionMappings { get; set; }
    }
}
