package com.andyadc.pomelo.menagerie.utils;

import com.andyadc.pomelo.menagerie.acl.ACLProvider;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.util.Collections;
import java.util.List;

/**
 * @author andaicheng
 * @version 2017/7/19
 */
public class ZKPath {

    /**
     * Zookeeper's path separator character.
     */
    public static final String PATH_SEPARATOR = "/";

    private ZKPath() {
    }

    /**
     * Make sure all the nodes in the path are created. NOTE: Unlike File.mkdirs(), Zookeeper doesn't distinguish
     * between directories and files. So, every node in the path is created. The data for each node is an empty blob
     *
     * @param zookeeper the client
     * @param path      path to ensure
     * @throws InterruptedException                 thread interruption
     * @throws org.apache.zookeeper.KeeperException Zookeeper errors
     */
    public static void mkdirs(ZooKeeper zookeeper, String path) throws InterruptedException, KeeperException {
        mkdirs(zookeeper, path, true, null);
    }

    /**
     * Make sure all the nodes in the path are created. NOTE: Unlike File.mkdirs(), Zookeeper doesn't distinguish
     * between directories and files. So, every node in the path is created. The data for each node is an empty blob
     *
     * @param zookeeper    the client
     * @param path         path to ensure
     * @param makeLastNode if true, all nodes are created. If false, only the parent nodes are created
     * @throws InterruptedException                 thread interruption
     * @throws org.apache.zookeeper.KeeperException Zookeeper errors
     */
    public static void mkdirs(ZooKeeper zookeeper, String path, boolean makeLastNode) throws InterruptedException, KeeperException {
        mkdirs(zookeeper, path, makeLastNode, null);
    }

    /**
     * Make sure all the nodes in the path are created. NOTE: Unlike File.mkdirs(), Zookeeper doesn't distinguish
     * between directories and files. So, every node in the path is created. The data for each node is an empty blob
     *
     * @param zookeeper    the client
     * @param path         path to ensure
     * @param makeLastNode if true, all nodes are created. If false, only the parent nodes are created
     * @param aclProvider  if not null, the ACL provider to use when creating parent nodes
     * @throws InterruptedException                 thread interruption
     * @throws org.apache.zookeeper.KeeperException Zookeeper errors
     */
    public static void mkdirs(ZooKeeper zookeeper, String path, boolean makeLastNode, ACLProvider aclProvider) throws InterruptedException, KeeperException {
        PathUtil.validatePath(path);

        int pos = 1; // skip first slash, root is guaranteed to exist
        do {
            pos = path.indexOf(PATH_SEPARATOR, pos + 1);

            if (pos == -1) {
                if (makeLastNode) {
                    pos = path.length();
                } else {
                    break;
                }
            }

            String subPath = path.substring(0, pos);
            if (zookeeper.exists(subPath, false) == null) {
                try {
                    List<ACL> acl = null;
                    if (aclProvider != null) {
                        acl = aclProvider.getAclForPath(subPath);
                        if (acl == null) {
                            acl = aclProvider.getDefaultAcl();
                        }
                    }
                    if (acl == null) {
                        acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
                    }
                    zookeeper.create(subPath, new byte[0], acl, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    // ignore... someone else has created it since we checked
                }
            }

        }
        while (pos < path.length());
    }

    /**
     * Return the children of the given path sorted by sequence number
     *
     * @param zookeeper the client
     * @param path      the path
     * @return sorted list of children
     * @throws InterruptedException                 thread interruption
     * @throws org.apache.zookeeper.KeeperException zookeeper errors
     */
    public static List<String> getSortedChildren(ZooKeeper zookeeper, String path) throws InterruptedException, KeeperException {
        List<String> children = zookeeper.getChildren(path, false);
        Collections.sort(children);
        return children;
    }

    /**
     * Recursively deletes children of a node.
     *
     * @param zookeeper  the client
     * @param path       path of the node to delete
     * @param deleteSelf flag that indicates that the node should also get deleted
     * @throws InterruptedException
     * @throws KeeperException
     */
    public static void deleteChildren(ZooKeeper zookeeper, String path, boolean deleteSelf) throws InterruptedException, KeeperException {
        PathUtil.validatePath(path);

        List<String> children = zookeeper.getChildren(path, null);
        for (String child : children) {
            String fullPath = makePath(path, child);
            deleteChildren(zookeeper, fullPath, true);
        }

        if (deleteSelf) {
            try {
                zookeeper.delete(path, -1);
            } catch (KeeperException.NotEmptyException e) {
                //someone has created a new child since we checked ... delete again.
                deleteChildren(zookeeper, path, true);
            } catch (KeeperException.NoNodeException e) {
                // ignore... someone else has deleted the node it since we checked
            }
        }
    }

    /**
     * Given a parent path and a child node, create a combined full path
     *
     * @param parent the parent
     * @param child  the child
     * @return full path
     */
    public static String makePath(String parent, String child) {
        StringBuilder path = new StringBuilder();

        joinPath(path, parent, child);

        return path.toString();
    }

    /**
     * Given a parent and a child node, join them in the given {@link StringBuilder path}
     *
     * @param path   the {@link StringBuilder} used to make the path
     * @param parent the parent
     * @param child  the child
     */
    private static void joinPath(StringBuilder path, String parent, String child) {
        // Add parent piece, with no trailing slash.
        if ((parent != null) && (parent.length() > 0)) {
            if (!parent.startsWith(PATH_SEPARATOR)) {
                path.append(PATH_SEPARATOR);
            }
            if (parent.endsWith(PATH_SEPARATOR)) {
                path.append(parent.substring(0, parent.length() - 1));
            } else {
                path.append(parent);
            }
        }

        if ((child == null) || (child.length() == 0) || (child.equals(PATH_SEPARATOR))) {
            // Special case, empty parent and child
            if (path.length() == 0) {
                path.append(PATH_SEPARATOR);
            }
            return;
        }

        // Now add the separator between parent and child.
        path.append(PATH_SEPARATOR);

        if (child.startsWith(PATH_SEPARATOR)) {
            child = child.substring(1);
        }

        if (child.endsWith(PATH_SEPARATOR)) {
            child = child.substring(0, child.length() - 1);
        }

        // Finally, add the child.
        path.append(child);
    }
}
