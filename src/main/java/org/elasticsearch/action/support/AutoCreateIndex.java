/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;

/**
 */
public class AutoCreateIndex {

    private final boolean needToCheck;
    private final boolean globallyDisabled;
    private final String[] matches;
    private final String[] matches2;

    /**
     * 构造 AutoCreateIndex
     * @param settings  Settings
     */
    public AutoCreateIndex(Settings settings) {
        String value = settings.get("action.auto_create_index");
        // 如果 action.auto_create_index 参数值是 null 或者明确为 true
        if (value == null || Booleans.isExplicitTrue(value)) {
            needToCheck = true;
            globallyDisabled = false;

            matches = null;
            matches2 = null;
        } else if (Booleans.isExplicitFalse(value)) {
            // 如果 action.auto_create_index 参数值明确是false
            needToCheck = false;
            globallyDisabled = true;

            matches = null;
            matches2 = null;
        } else {
            needToCheck = true;
            globallyDisabled = false;

            // 使用逗号分割
            matches = Strings.commaDelimitedListToStringArray(value);
            // 每个分割字符去掉首字符
            matches2 = new String[matches.length];
            for (int i = 0; i < matches.length; i++) {
                matches2[i] = matches[i].substring(1);
            }
        }
    }

    /**
     * Do we really need to check if an index should be auto created?
     */
    public boolean needToCheck() {
        return this.needToCheck;
    }

    /**
     * 是否应该自动创建索引
     * Should the index be auto created?
     */
    public boolean shouldAutoCreate(String index, ClusterState state) {
        // action.auto_create_index 是 false
        // 不再继续检查, 无法自动创建索引
        if (!needToCheck) {
            return false;
        }
        // 如果索引或者别名中已经包含了index
        if (state.metaData().hasConcreteIndex(index)) {
            return false;
        }
        // action.auto_create_index 是 false
        if (globallyDisabled) {
            return false;
        }
        // matches not set, default value of "true"
        // action.auto_create_index 是 null 或者是 true 或者是 false
        if (matches == null) {
            return true;
        }
        // 正则条件判断index是否满足action.auto_create_index
        for (int i = 0; i < matches.length; i++) {
            char c = matches[i].charAt(0);
            if (c == '-') {
                if (Regex.simpleMatch(matches2[i], index)) {
                    return false;
                }
            } else if (c == '+') {
                if (Regex.simpleMatch(matches2[i], index)) {
                    return true;
                }
            } else {
                if (Regex.simpleMatch(matches[i], index)) {
                    return true;
                }
            }
        }
        return false;
    }
}
