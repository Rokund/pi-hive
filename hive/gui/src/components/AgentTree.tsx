import { useEffect, useMemo, useState } from "react";
import type { AgentNode, AgentStatus } from "../types";

interface AgentTreeProps {
  tree: AgentNode[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
  onRename: (id: string, name: string) => void;
}

const STATUS_LABEL: Record<AgentStatus, string> = {
  running: "running",
  idle: "idle",
  done: "done",
  failed: "failed",
  aborted: "aborted",
};

function StatusBadge({ status }: { status: AgentStatus }) {
  const cls =
    status === "running"
      ? "status-running"
      : status === "idle"
        ? "status-idle"
        : status === "done"
          ? "status-done"
          : status === "failed"
            ? "status-failed"
            : status === "aborted"
              ? "status-aborted"
              : "status-unknown";
  return (
    <span className={`status-dot ${cls}`} title={STATUS_LABEL[status]} />
  );
}

interface TreeNodeProps {
  node: AgentNode;
  childrenById: Map<string, AgentNode[]>;
  selectedId: string | null;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
  depth: number;
  menuFor: string | null;
  renaming: string | null;
  onToggleMenu: (id: string) => void;
  onCloseMenu: () => void;
  onStartRename: (id: string) => void;
  onCancelRename: () => void;
  onRenameSubmit: (id: string, name: string) => void;
}

function TreeNode({
  node,
  childrenById,
  selectedId,
  onSelect,
  onDelete,
  depth,
  menuFor,
  renaming,
  onToggleMenu,
  onCloseMenu,
  onStartRename,
  onCancelRename,
  onRenameSubmit,
}: TreeNodeProps) {
  const children = childrenById.get(node.id) ?? [];
  const selected = selectedId === node.id;
  const menuOpen = menuFor === node.id;
  const isRenaming = renaming === node.id;

  return (
    <div className="tree-node">
      <div
        className={`tree-row${selected ? " selected" : ""}`}
        style={{ paddingLeft: 8 + depth * 16 }}
        role="button"
        tabIndex={0}
        onClick={() => onSelect(node.id)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onSelect(node.id);
          }
        }}
      >
        <StatusBadge status={node.status} />
        <span className="tree-name">
          {node.name}
          {node.kind === "subagent" ? (
            <span className="tree-kind">sub</span>
          ) : null}
        </span>
        {node.loaded === false && node.sessionFile ? (
          <span className="tree-archived" title="Archived session — loads when opened">
            archived
          </span>
        ) : null}
        <span className="tree-menu-wrap">
          <button
            type="button"
            className="tree-menu-btn"
            onClick={(e) => {
              e.stopPropagation();
              onToggleMenu(node.id);
            }}
            title="Session actions"
          >
            …
          </button>
          {menuOpen && (
            <>
              <div className="tree-menu-overlay" onClick={onCloseMenu} />
              <div
                className="tree-menu"
                onClick={(e) => e.stopPropagation()}
              >
                {isRenaming ? (
                  <form
                    className="rename-form"
                    onSubmit={(e) => {
                      e.preventDefault();
                      const v = (
                        e.currentTarget.elements.namedItem(
                          "name",
                        ) as HTMLInputElement
                      )?.value.trim();
                      if (v) onRenameSubmit(node.id, v);
                      else onCancelRename();
                    }}
                  >
                    <input
                      name="name"
                      defaultValue={node.name}
                      autoFocus
                      onKeyDown={(e) => {
                        if (e.key === "Escape") onCancelRename();
                      }}
                    />
                    <button type="submit" className="rename-save">
                      Save
                    </button>
                  </form>
                ) : (
                  <>
                    <button
                      type="button"
                      className="menu-item"
                      onClick={() => onStartRename(node.id)}
                    >
                      Rename session
                    </button>
                    <button
                      type="button"
                      className="menu-item danger"
                      onClick={() => onDelete(node.id)}
                    >
                      Delete session
                    </button>
                  </>
                )}
              </div>
            </>
          )}
        </span>
      </div>
      {children.length > 0 ? (
        <div className="tree-children">
          {children.map((child) => (
            <TreeNode
              key={child.id}
              node={child}
              childrenById={childrenById}
              selectedId={selectedId}
              onSelect={onSelect}
              onDelete={onDelete}
              depth={depth + 1}
              menuFor={menuFor}
              renaming={renaming}
              onToggleMenu={onToggleMenu}
              onCloseMenu={onCloseMenu}
              onStartRename={onStartRename}
              onCancelRename={onCancelRename}
              onRenameSubmit={onRenameSubmit}
            />
          ))}
        </div>
      ) : null}
    </div>
  );
}

/**
 * Renders the agent tree. The hive sends a flat list of `AgentNode` objects in
 * `hive:tree`; parent/child relationships are reconstructed from `parentId` /
 * `childrenIds`. Primary agents are roots; subagents nest beneath their parent.
 *
 * Each row carries a "…" menu (at the right edge, where the session id used to
 * be) with Rename session / Delete session actions.
 */
export default function AgentTree({
  tree,
  selectedId,
  onSelect,
  onDelete,
  onRename,
}: AgentTreeProps) {
  const [menuFor, setMenuFor] = useState<string | null>(null);
  const [renaming, setRenaming] = useState<string | null>(null);

  const { roots, childrenById } = useMemo(() => {
    const ordered = new Map<string, AgentNode[]>();
    for (const node of tree) {
      const arr = node.childrenIds.map((cid) => tree.find((t) => t.id === cid)).filter(
        Boolean,
      ) as AgentNode[];
      ordered.set(node.id, arr);
    }
    const roots = tree.filter((n) => !n.parentId);
    return { roots, childrenById: ordered };
  }, [tree]);

  // Close any open menu / rename form when the tree changes (e.g. a rename or
  // delete broadcast lands) so stale menus never linger over different nodes.
  useEffect(() => {
    setMenuFor(null);
    setRenaming(null);
  }, [tree]);

  if (tree.length === 0) {
    return <div className="empty">No agents connected yet.</div>;
  }

  return (
    <div className="agent-tree">
      {roots.map((root) => (
        <TreeNode
          key={root.id}
          node={root}
          childrenById={childrenById}
          selectedId={selectedId}
          onSelect={onSelect}
          onDelete={onDelete}
          depth={0}
          menuFor={menuFor}
          renaming={renaming}
          onToggleMenu={(id) => {
            setMenuFor((m) => (m === id ? null : id));
            setRenaming(null);
          }}
          onCloseMenu={() => {
            setMenuFor(null);
            setRenaming(null);
          }}
          onStartRename={(id) => setRenaming(id)}
          onCancelRename={() => setRenaming(null)}
          onRenameSubmit={(id, name) => {
            onRename(id, name);
            setRenaming(null);
            setMenuFor(null);
          }}
        />
      ))}
    </div>
  );
}
