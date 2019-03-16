// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Extensions.Internal;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors
{
    public partial class NavigationExpandingVisitor : LinqQueryVisitorBase
    {
        private IModel _model;

        public NavigationExpandingVisitor(IModel model)
        {
            _model = model;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
            {
                return navigationBindingExpression;
            }

            if (extensionExpression is CustomRootExpression customRootExpression)
            {
                return customRootExpression;
            }

            //if (extensionExpression is CustomRootExpression2 customRootExpression2)
            //{
            //    return customRootExpression2;
            //}

            return base.VisitExtension(extensionExpression);
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            var newExpression = Visit(memberExpression.Expression);
            if (newExpression is NavigationExpansionExpression navigationExpansionExpression
                && navigationExpansionExpression.State.PendingTerminatingOperator != null)
            {
                var selectorParameter = Expression.Parameter(newExpression.Type, navigationExpansionExpression.State.CurrentParameter.Name);
                var selector = Expression.Lambda(
                    Expression.MakeMemberAccess(
                        selectorParameter,
                        memberExpression.Member),
                    selectorParameter);

                var remappedSelectorBody = ExpressionExtensions.CombineAndRemapLambdas(navigationExpansionExpression.State.PendingSelector, selector, selectorParameter).Body;

                var binder = new NavigationPropertyBindingVisitor(
                    navigationExpansionExpression.State.CurrentParameter,
                    navigationExpansionExpression.State.SourceMappings);

                var boundSelectorBody = binder.Visit(remappedSelectorBody);
                if (boundSelectorBody is NavigationBindingExpression navigationBindingExpression
                    && navigationBindingExpression.NavigationTreeNode.Navigation is INavigation lastNavigation
                    && lastNavigation != null)
                {
                    if (lastNavigation.IsCollection())
                    {
                        var collectionNavigationElementType = lastNavigation.ForeignKey.DeclaringEntityType.ClrType;
                        var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(collectionNavigationElementType);
                        var outerParameter = Expression.Parameter(collectionNavigationElementType, collectionNavigationElementType.GenerateParameterName());

                        var outerKeyAccess = NavigationExpansionHelpers.CreateKeyAccessExpression(
                            outerParameter,
                            lastNavigation.ForeignKey.Properties);

                        var innerParameter = Expression.Parameter(navigationExpansionExpression.Type);
                        var innerKeyAccessLambda = Expression.Lambda(
                            NavigationExpansionHelpers.CreateKeyAccessExpression(
                                innerParameter,
                                lastNavigation.ForeignKey.PrincipalKey.Properties),
                            innerParameter);

                        var combinedKeySelectorBody = ExpressionExtensions.CombineAndRemapLambdas(navigationExpansionExpression.State.PendingSelector, innerKeyAccessLambda).Body;

                        // TODO: properly compare combinedKeySelectorBody with outerKeyAccess for nullability match
                        if (outerKeyAccess.Type != combinedKeySelectorBody.Type)
                        {
                            if (combinedKeySelectorBody.Type.IsNullableType())
                            {
                                outerKeyAccess = Expression.Convert(outerKeyAccess, combinedKeySelectorBody.Type);
                            }
                            else
                            {
                                combinedKeySelectorBody = Expression.Convert(combinedKeySelectorBody, outerKeyAccess.Type);
                            }
                        }

                        var rewrittenState = new NavigationExpansionExpressionState(
                            navigationExpansionExpression.State.CurrentParameter,
                            navigationExpansionExpression.State.SourceMappings,
                            Expression.Lambda(combinedKeySelectorBody, navigationExpansionExpression.State.CurrentParameter),
                            applyPendingSelector: true,
                            navigationExpansionExpression.State.PendingTerminatingOperator,
                            navigationExpansionExpression.State.CustomRootMappings/*,
                        navigationExpansionExpression.State.NestedExpansionMappings*/);

                        var rewrittenNavigationExpansionExpression = new NavigationExpansionExpression(navigationExpansionExpression.Operand, rewrittenState, combinedKeySelectorBody.Type);
                        var inner = new NavigationExpansionReducingVisitor().Visit(rewrittenNavigationExpansionExpression);

                        var predicate = Expression.Lambda(
                            Expression.Equal(outerKeyAccess, inner),
                            outerParameter);

                        var whereMethodInfo = QueryableWhereMethodInfo.MakeGenericMethod(collectionNavigationElementType);
                        var rewritten = Expression.Call(
                            whereMethodInfo,
                            entityQueryable,
                            predicate);

                        var entityType = lastNavigation.ForeignKey.DeclaringEntityType;

                        // TODO: copied from visit constant - DRY !!!!
                        var sourceMapping = new SourceMapping
                        {
                            RootEntityType = entityType,
                        };

                        var navigationTreeRoot = NavigationTreeNode.CreateRoot(sourceMapping, fromMapping: new List<string>(), optional: false);
                        sourceMapping.NavigationTree = navigationTreeRoot;

                        var pendingSelectorParameter = Expression.Parameter(entityType.ClrType);
                        var pendingSelector = Expression.Lambda(
                            new NavigationBindingExpression(
                                pendingSelectorParameter,
                                navigationTreeRoot,
                                entityType,
                                sourceMapping,
                                pendingSelectorParameter.Type),
                            pendingSelectorParameter);

                        var result = new NavigationExpansionExpression(
                            rewritten,
                            new NavigationExpansionExpressionState(
                                pendingSelectorParameter,
                                new List<SourceMapping> { sourceMapping },
                                pendingSelector,
                                applyPendingSelector: false,
                                pendingTerminatingOperator: null, // TODO: incorrect?
                                customRootMappings: new List<List<string>>()/*,
                            nestedExpansionMappings: new List<NestedExpansionMapping>()*/),
                            rewritten.Type);

                        return result;
                    }
                    else
                    {
                        return ProcessSelectCore(
                            navigationExpansionExpression.Operand,
                            navigationExpansionExpression.State,
                            selector,
                            memberExpression.Type);
                    }
                }

                // TODO idk if thats needed
                var newState = new NavigationExpansionExpressionState(
                    navigationExpansionExpression.State.CurrentParameter,
                    navigationExpansionExpression.State.SourceMappings,
                    Expression.Lambda(boundSelectorBody, navigationExpansionExpression.State.CurrentParameter),
                    applyPendingSelector: true,
                    navigationExpansionExpression.State.PendingTerminatingOperator,
                    navigationExpansionExpression.State.CustomRootMappings/*,
                    navigationExpansionExpression.State.NestedExpansionMappings*/);

                // TODO: expand navigations

                return new NavigationExpansionExpression(
                    navigationExpansionExpression.Operand,
                    newState,
                    memberExpression.Type);
            }

            return base.VisitMember(memberExpression);
        }

        protected override Expression VisitBinary(BinaryExpression binaryExpression)
        {
            if (binaryExpression.NodeType == ExpressionType.Equal
                || binaryExpression.NodeType == ExpressionType.NotEqual)
            {
                var newLeft = Visit(binaryExpression.Left);
                var newRight = Visit(binaryExpression.Right);

                var leftConstantNull = newLeft.IsNullConstantExpression();
                var rightConstantNull = newRight.IsNullConstantExpression();

                var leftNavigationExpansionExpression = newLeft as NavigationExpansionExpression;
                var rightNavigationExpansionExpression = newRight as NavigationExpansionExpression;
                var leftNavigationBindingExpression = default(NavigationBindingExpression);
                var rightNavigationBindingExpression = default(NavigationBindingExpression);

                if (leftNavigationExpansionExpression?.State.PendingTerminatingOperator != null)
                {
                    leftNavigationBindingExpression = leftNavigationExpansionExpression.State.PendingSelector.Body as NavigationBindingExpression;
                }

                if (rightNavigationExpansionExpression?.State.PendingTerminatingOperator != null)
                {
                    rightNavigationBindingExpression = rightNavigationExpansionExpression.State.PendingSelector.Body as NavigationBindingExpression;
                }

                if (leftNavigationBindingExpression != null
                    && rightConstantNull)
                {
                    var outerKeyAccess = NavigationExpansionHelpers.CreateKeyAccessExpression(
                        leftNavigationBindingExpression,
                        leftNavigationBindingExpression.EntityType.FindPrimaryKey().Properties);

                    var innerKeyAccess = NavigationExpansionHelpers.CreateNullKeyExpression(
                        outerKeyAccess.Type,
                        leftNavigationBindingExpression.EntityType.FindPrimaryKey().Properties.Count);

                    // TODO: null key is typed as object - need to properly resolve the potential type discrepancy
                    //if (outerKeyAccess.Type != innerKeyAccess.Type)
                    //{
                    //    if (outerKeyAccess.Type.IsNullableType())
                    //    {
                    //        innerKeyAccess = Expression.Convert(innerKeyAccess, outerKeyAccess.Type);
                    //    }
                    //    else
                    //    {
                    //        outerKeyAccess = Expression.Convert(outerKeyAccess, innerKeyAccess.Type);
                    //    }
                    //}

                    var newLeftNavigationExpansionExpressionState = new NavigationExpansionExpressionState(
                        leftNavigationExpansionExpression.State.CurrentParameter,
                        leftNavigationExpansionExpression.State.SourceMappings,
                        Expression.Lambda(outerKeyAccess, leftNavigationExpansionExpression.State.PendingSelector.Parameters[0]),
                        applyPendingSelector: true,
                        leftNavigationExpansionExpression.State.PendingTerminatingOperator,
                        leftNavigationExpansionExpression.State.CustomRootMappings);

                    newLeft = new NavigationExpansionExpression(
                        leftNavigationExpansionExpression.Operand,
                        newLeftNavigationExpansionExpressionState,
                        outerKeyAccess.Type);

                    newRight = innerKeyAccess;
                }

                var result = binaryExpression.NodeType == ExpressionType.Equal
                    ? Expression.Equal(newLeft, newRight)
                    : Expression.NotEqual(newLeft, newRight);

                return result;
            }

            return base.VisitBinary(binaryExpression);
        }

        protected /*override*/ Expression VisitBinary2(BinaryExpression binaryExpression)
        {
            // entity.CollectionNavigation == null <==> entity == null
            // entity1.CollectionNavigation == entity2.CollectionNavigation <==> entity1 == entity2

            // TODO: there is a lot of duplication with NavigationComparisonOptimizingVisitor - DRY!
            if (binaryExpression.NodeType == ExpressionType.Equal
                || binaryExpression.NodeType == ExpressionType.NotEqual)
            {
                var leftConstantNull = binaryExpression.Left.IsNullConstantExpression();
                var rightConstantNull = binaryExpression.Right.IsNullConstantExpression();

                var leftParent = default(Expression);
                var leftNavigation = default(INavigation);
                var rightParent = default(Expression);
                var rightNavigation = default(INavigation);

                // TODO: this is hacky and won't work for weak entity types
                // also, add support for EF.Property and maybe convert node around the navigation
                if (binaryExpression.Left is MemberExpression leftMember
                    && leftMember.Type.TryGetSequenceType() is Type leftSequenceType
                    && leftSequenceType != null
                    && _model.FindEntityType(leftMember.Expression.Type) is IEntityType leftParentEntityType)
                {
                    leftNavigation = leftParentEntityType.FindNavigation(leftMember.Member.Name);
                    if (leftNavigation != null)
                    {
                        leftParent = leftMember.Expression;
                    }
                }

                if (binaryExpression.Right is MemberExpression rightMember
                    && rightMember.Type.TryGetSequenceType() is Type rightSequenceType
                    && rightSequenceType != null
                    && _model.FindEntityType(rightMember.Expression.Type) is IEntityType rightParentEntityType)
                {
                    rightNavigation = rightParentEntityType.FindNavigation(rightMember.Member.Name);
                    if (rightNavigation != null)
                    {
                        rightParent = rightMember.Expression;
                    }
                }

                if (binaryExpression.Left is ConstantExpression leftConstant
                    && leftConstant.Value == null)
                {
                    leftConstantNull = true;
                }

                if (binaryExpression.Right is ConstantExpression rightConstant
                    && rightConstant.Value == null)
                {
                    rightConstantNull = true;
                }

                if (leftNavigation != null
                    && leftNavigation.IsCollection()
                    && leftNavigation == rightNavigation)
                {
                    var rewritten = Expression.MakeBinary(binaryExpression.NodeType, leftParent, rightParent);

                    return Visit(rewritten);
                }

                if (leftNavigation != null
                    && leftNavigation.IsCollection()
                    && rightConstantNull)
                {
                    var rewritten = Expression.MakeBinary(binaryExpression.NodeType, leftParent, Expression.Constant(null));

                    return Visit(rewritten);
                }

                if (rightNavigation != null
                    && rightNavigation.IsCollection()
                    && leftConstantNull)
                {
                    var rewritten = Expression.MakeBinary(binaryExpression.NodeType, Expression.Constant(null), rightParent);

                    return Visit(rewritten);
                }
            }

            return base.VisitBinary(binaryExpression);
        }
    }
}
