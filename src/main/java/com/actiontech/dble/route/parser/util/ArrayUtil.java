/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class ArrayUtil {
    private ArrayUtil() {
    }

    public static boolean equals(String str1, String str2) {
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equals(str2);
    }

    public static boolean contains(String[] list, String str) {
        if (list == null) {
            return false;
        }
        for (String string : list) {
            if (equals(str, string)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(ArrayList<String> list, String str) {
        if (list == null) {
            return false;
        }
        for (String string : list) {
            if (equals(str, string)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containAll(ArrayList list, ArrayList child) {
        for (Object co : child) {
            boolean found = false;
            for (Object fo : list) {
                if (co.equals(fo)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }


    public static boolean containDuplicate(List list, Object obj) {
        boolean findOne = false;
        for (Object x : list) {
            if (x.equals(obj)) {
                if (!findOne) {
                    findOne = true;
                } else {
                    return true;
                }
            }
        }
        return false;
    }

}
