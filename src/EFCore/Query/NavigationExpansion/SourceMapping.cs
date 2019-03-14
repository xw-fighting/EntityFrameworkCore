// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion
{
    public class SourceMapping
    {
        public IEntityType RootEntityType { get; set; }

        public NavigationTreeNode NavigationTree { get; set; }
    }

    public class NestedExpansionMapping
    {
        public NestedExpansionMapping(List<string> path, NavigationExpansionExpression expansion)
        {
            Path = path;
            Expansion = expansion;
        }

        public List<string> Path { get; set; }
        public NavigationExpansionExpression Expansion { get; set; }
    }
}
