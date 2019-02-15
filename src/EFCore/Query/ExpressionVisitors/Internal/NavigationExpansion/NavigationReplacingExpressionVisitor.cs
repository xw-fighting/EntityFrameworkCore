// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    public class NavigationReplacingExpressionVisitor : NavigationExpansionExpressionVisitorBase
    {
        private ParameterExpression _previousParameter;
        private ParameterExpression _newParameter;

        public NavigationReplacingExpressionVisitor(
            ParameterExpression previousParameter,
            ParameterExpression newParameter)
        {
            _previousParameter = previousParameter;
            _newParameter = newParameter;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
            {
                if (navigationBindingExpression.RootParameter == _previousParameter)
                {
                    var transparentIdentifierAccessorPath = navigationBindingExpression.SourceMapping.TransparentIdentifierMapping.Where(
                        m => m.navigations.Count == navigationBindingExpression.Navigations.Count
                        && m.navigations.Zip(navigationBindingExpression.Navigations, (o, i) => o == i).All(e => e)).SingleOrDefault().path;

                    //var path2 = FindResultPath(navigationBindingExpression.SourceMapping, navigationBindingExpression.Navigations);

                    if (transparentIdentifierAccessorPath != null)
                    {
                        var result = BuildTransparentIdentifierAccessorExpression(_newParameter, navigationBindingExpression.SourceMapping.InitialPath, transparentIdentifierAccessorPath);

                        return result;
                    }
                }

                return navigationBindingExpression;
            }

            return base.VisitExtension(extensionExpression);
        }

        //private List<string> FindResultPath(SourceMapping sourceMapping, IEnumerable<INavigation> navigations)
        //{
        //    foreach (var rootFromMapping in sourceMapping.RootFromMappings)
        //    {
        //        if (rootFromMapping.SequenceEqual(navigations.Select(n => n.Name)))
        //        {
        //            return sourceMapping.RootToMapping;
        //        }
        //    }

        //    foreach (var navigationTree in sourceMapping.FoundNavigations)
        //    {
        //        var result = FindResultPath(navigationTree, navigations);
        //        if (result != null)
        //        {
        //            return result;
        //        }
        //    }

        //    throw new InvalidOperationException("Couldn't find mapping for: " + string.Join(".", navigations.Select(n => n.Name)));
        //}

        //private List<string> FindResultPath(NavigationTreeNode navigationTreeNode, IEnumerable<INavigation> navigations)
        //{
        //    foreach (var mapping in navigationTreeNode.FromMappings)
        //    {
        //        if (mapping.SequenceEqual(navigations.Select(n => n.Name)))
        //        {
        //            return navigationTreeNode.ToMapping;
        //        }
        //    }

        //    foreach (var child in navigationTreeNode.Children)
        //    {
        //        var result = FindResultPath(child, navigations);
        //        if (result != null)
        //        {
        //            return result;
        //        }
        //    }

        //    return null;
        //}

        protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        {
            var newParameters = new List<ParameterExpression>();
            var parameterChanged = false;

            foreach (var parameter in lambdaExpression.Parameters)
            {
                if (parameter == _previousParameter
                    && parameter != _newParameter)
                {
                    newParameters.Add(_newParameter);
                    parameterChanged = true;
                }
                else
                {
                    newParameters.Add(parameter);
                }
            }

            var newBody = Visit(lambdaExpression.Body);

            return parameterChanged || newBody != lambdaExpression.Body
                ? Expression.Lambda(newBody, newParameters)
                : lambdaExpression;
        }

        // TODO: DRY
        private Expression BuildTransparentIdentifierAccessorExpression(Expression source, List<string> initialPath, List<string> accessorPath)
        {
            var result = source;

            var fullPath = accessorPath;// initialPath != null
                //? initialPath.Concat(accessorPath).ToList()
                //: accessorPath;

            if (fullPath != null)
            {
                foreach (var accessorPathElement in fullPath)
                {
                    // TODO: nasty hack, clean this up!!!!
                    if (result.Type.GetProperties().Any(p => p.Name == accessorPathElement))
                    {
                        result = Expression.Property(result, accessorPathElement);
                    }
                    else
                    {
                        result = Expression.Field(result, accessorPathElement);
                    }
                }
            }

            return result;
        }
    }

    public class NavigationReplacingExpressionVisitor2 : NavigationExpansionExpressionVisitorBase
    {
        private ParameterExpression _previousParameter;
        private ParameterExpression _newParameter;

        public NavigationReplacingExpressionVisitor2(
            ParameterExpression previousParameter,
            ParameterExpression newParameter)
        {
            _previousParameter = previousParameter;
            _newParameter = newParameter;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression2 navigationBindingExpression)
            {
                if (navigationBindingExpression.RootParameter == _previousParameter)
                {
                    var path = navigationBindingExpression.NavigationTreeNode.ToMapping;
                    var result = BuildAccessorExpression(_newParameter, path);

                    return result;
                }

                return navigationBindingExpression;
            }

            return base.VisitExtension(extensionExpression);
        }

        protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        {
            var newParameters = new List<ParameterExpression>();
            var parameterChanged = false;

            foreach (var parameter in lambdaExpression.Parameters)
            {
                if (parameter == _previousParameter
                    && parameter != _newParameter)
                {
                    newParameters.Add(_newParameter);
                    parameterChanged = true;
                }
                else
                {
                    newParameters.Add(parameter);
                }
            }

            var newBody = Visit(lambdaExpression.Body);

            return parameterChanged || newBody != lambdaExpression.Body
                ? Expression.Lambda(newBody, newParameters)
                : lambdaExpression;
        }

        // TODO: DRY
        private Expression BuildAccessorExpression(Expression source, List<string> path)
        {
            var result = source;
            foreach (var accessorPathElement in path)
            {
                // TODO: nasty hack, clean this up!!!!
                if (result.Type.GetProperties().Any(p => p.Name == accessorPathElement))
                {
                    result = Expression.Property(result, accessorPathElement);
                }
                else
                {
                    result = Expression.Field(result, accessorPathElement);
                }
            }

            return result;
        }
    }
}
