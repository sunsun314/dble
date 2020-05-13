/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.common.config;

public interface ProblemReporter {
    void error(String problem);

    void warn(String problem);

    void notice(String problem);
}
