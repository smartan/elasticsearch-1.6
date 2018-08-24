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

package org.elasticsearch.common.cli;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import java.util.Collection;

/**
 *
 */
public class CliToolConfig {

    public static Builder config(String name, Class<? extends CliTool> toolType) {
        return new Builder(name, toolType);
    }

    private final Class<? extends CliTool> toolType;
    private final String name;
    private final ImmutableMap<String, Cmd> cmds;

    private static final HelpPrinter helpPrinter = new HelpPrinter();

    private CliToolConfig(String name, Class<? extends CliTool> toolType, Cmd[] cmds) {
        this.name = name;
        this.toolType = toolType;
        ImmutableMap.Builder<String, Cmd> cmdsBuilder = ImmutableMap.builder();
        for (int i = 0; i < cmds.length; i++) {
            cmdsBuilder.put(cmds[i].name, cmds[i]);
        }
        this.cmds = cmdsBuilder.build();
    }

    public boolean isSingle() {
        return cmds.size() == 1;
    }

    public Cmd single() {
        assert isSingle() : "Requesting single command on a multi-command tool";
        return cmds.values().iterator().next();
    }

    public Class<? extends CliTool> toolType() {
        return toolType;
    }

    public String name() {
        return name;
    }

    public Collection<Cmd> cmds() {
        return cmds.values();
    }

    public Cmd cmd(String name) {
        return cmds.get(name);
    }

    public void printUsage(Terminal terminal) {
        helpPrinter.print(this, terminal);
    }

    public static class Builder {

        public static Cmd.Builder cmd(String name, Class<? extends CliTool.Command> cmdType) {
            return new Cmd.Builder(name, cmdType);
        }

        public static OptionBuilder option(String shortName, String longName) {
            return new OptionBuilder(shortName, longName);
        }

        public static OptionGroupBuilder optionGroup(boolean required) {
            return new OptionGroupBuilder(required);
        }

        private final Class<? extends CliTool> toolType;
        private final String name;
        private Cmd[] cmds;

        private Builder(String name, Class<? extends CliTool> toolType) {
            this.name = name;
            this.toolType = toolType;
        }

        public Builder cmds(Cmd.Builder... cmds) {
            this.cmds = new Cmd[cmds.length];
            for (int i = 0; i < cmds.length; i++) {
                this.cmds[i] = cmds[i].build();
                this.cmds[i].toolName = name;
            }
            return this;
        }

        public Builder cmds(Cmd... cmds) {
            for (int i = 0; i < cmds.length; i++) {
                cmds[i].toolName = name;
            }
            this.cmds = cmds;
            return this;
        }

        public CliToolConfig build() {
            return new CliToolConfig(name, toolType, cmds);
        }
    }

    public static class Cmd {

        private String toolName;
        private final String name;
        private final Class<? extends CliTool.Command> cmdType;
        private final Options options;

        private Cmd(String name, Class<? extends CliTool.Command> cmdType, Options options) {
            this.name = name;
            this.cmdType = cmdType;
            this.options = options;
            OptionsSource.VERBOSITY.populate(options);
        }

        public Class<? extends CliTool.Command> cmdType() {
            return cmdType;
        }

        public String name() {
            return name;
        }

        public Options options() {
            // TODO Remove this when commons-cli 1.3 will be released
            // and replace by return options;
            // See https://issues.apache.org/jira/browse/CLI-183
            Options copy = new Options();
            for (Object oOption : options.getOptions()) {
                Option option = (Option) oOption;
                copy.addOption(option);
            }
            OptionsSource.VERBOSITY.populate(copy);
            return copy;
        }

        public void printUsage(Terminal terminal) {
            helpPrinter.print(toolName, this, terminal);
        }

        public static class Builder {

            private final String name;
            private final Class<? extends CliTool.Command> cmdType;
            private Options options = new Options();

            private Builder(String name, Class<? extends CliTool.Command> cmdType) {
                this.name = name;
                this.cmdType = cmdType;
            }

            public Builder options(OptionBuilder... optionBuilder) {
                for (int i = 0; i < optionBuilder.length; i++) {
                    options.addOption(optionBuilder[i].build());
                }
                return this;
            }

            public Builder optionGroups(OptionGroupBuilder... optionGroupBuilders) {
                for (OptionGroupBuilder builder : optionGroupBuilders) {
                    options.addOptionGroup(builder.build());
                }
                return this;
            }

            public Cmd build() {
                return new Cmd(name, cmdType, options);
            }
        }
    }

    public static class OptionBuilder {

        private final Option option;

        private OptionBuilder(String shortName, String longName) {
            option = new Option(shortName, "");
            option.setLongOpt(longName);
            option.setArgName(longName);
        }

        public OptionBuilder required(boolean required) {
            option.setRequired(required);
            return this;
        }

        public OptionBuilder hasArg(boolean optional) {
            option.setOptionalArg(optional);
            option.setArgs(1);
            return this;
        }

        public Option build() {
            return option;
        }
    }

    public static class OptionGroupBuilder {

        private OptionGroup group;

        private OptionGroupBuilder(boolean required) {
            group = new OptionGroup();
            group.setRequired(required);
        }

        public OptionGroupBuilder options(OptionBuilder... optionBuilders) {
            for (OptionBuilder builder : optionBuilders) {
                group.addOption(builder.build());
            }
            return this;
        }

        public OptionGroup build() {
            return group;
        }

    }

    static abstract class OptionsSource {

        static final OptionsSource HELP = new OptionsSource() {

            @Override
            void populate(Options options) {
                options.addOption(new OptionBuilder("h", "help").required(false).build());
            }
        };

        static final OptionsSource VERBOSITY = new OptionsSource() {
            @Override
            void populate(Options options) {
                OptionGroup verbosityGroup = new OptionGroup();
                verbosityGroup.setRequired(false);
                verbosityGroup.addOption(new OptionBuilder("s", "silent").required(false).build());
                verbosityGroup.addOption(new OptionBuilder("v", "verbose").required(false).build());
                options.addOptionGroup(verbosityGroup);
            }
        };

        private Options options;

        Options options() {
            if (options == null) {
                options = new Options();
                populate(options);
            }
            return options;
        }

        abstract void populate(Options options);

    }
}
