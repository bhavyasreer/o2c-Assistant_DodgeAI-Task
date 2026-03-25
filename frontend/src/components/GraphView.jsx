// GraphView.jsx — Fixed & Enhanced
// ─── FIXES APPLIED ────────────────────────────────────────────────────────────
// [F1] Root nodes NEVER removed — rootIdsRef guards every collapse/filter
// [F2] Controlled expansion — only immediate neighbours added, no duplicates
// [F3] activeNodeId + activePath state — full path root→node tracked
// [F4] Fade (opacity 0.15) instead of removal for out-of-path nodes
// [F5] Hierarchical lane layout (DAGRE-style column per type) + resolveCollisions
// [F6] Smooth zoom with maxZoom cap + fitBounds instead of setCenter snap
// [F7] Full flow path highlight (all ancestors + descendants of clicked node)
// [F8] Chat highlight: extract IDs, highlight all matched + chain edges
// [F9] Hover tooltip with metadata card
// [F10] Memoized nodesWithHandlers / edgesWithStyle — no full recompute on click

import { useEffect, useState, useCallback, useRef, useMemo } from 'react'
import ReactFlow, {
  Background,
  Controls,
  Handle,
  useNodesState,
  useEdgesState,
  MarkerType,
  BackgroundVariant,
  Position,
  useReactFlow,
  ReactFlowProvider,
} from 'reactflow'
import 'reactflow/dist/style.css'
import axios from 'axios'

// When deployed behind the same host (FastAPI serving React), this should stay empty
// so requests go to `/api/...`. Locally you can set `VITE_API_BASE_URL=http://localhost:8000`.
const API = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '')

// ─── COLORS ──────────────────────────────────────────────────────────────────
const C = {
  customer:         '#3b82f6',
  order:            '#10b981',
  product:          '#ec4899',
  delivery:         '#f59e0b',
  billing:          '#ef4444',
  payment:          '#6b7280',
  customer_company: '#0ea5a3',
  sales_area:       '#6366f1',
  address:          '#8b5cf6',
  plant:            '#14b8a6',
  schedule_line:    '#64748b',
}

const LABELS = {
  places:       'places',
  contains:     'contains',
  fulfilled_by: 'fulfils',
  billed_as:    'billed',
  paid_by:      'paid',
}

// ─── LAYOUT CONSTANTS ────────────────────────────────────────────────────────
const LABEL_ZOOM_THRESHOLD = 0.45

// [F5] Hierarchical lane X positions (LEFT→RIGHT flow)
const LANE_X_BY_TYPE = {
  customer:         0,
  order:            340,
  product:          640,
  delivery:         980,
  billing:          1300,
  payment:          1620,
  customer_company: 0,
  sales_area:       340,
  address:          640,
  plant:            980,
  schedule_line:    1300,
}

function getLaneX(type) {
  return LANE_X_BY_TYPE[type] ?? 600
}

// ─── [F5] PLACE CHILDREN — collision-aware placement in a lane ───────────────
function placeChildren(parentPos, children, existingNodes, opts = {}) {
  const count  = children.length
  const laneX  = opts.laneX ?? (parentPos.x + 340)
  const spreadY = opts.spreadY ?? 110

  const minDx = 240
  const minDy = 90

  const allOccupied = existingNodes.map(n => ({ x: n.position.x, y: n.position.y }))
  const planned = []

  const isFree = (x, y) => {
    for (const p of allOccupied) {
      if (Math.abs(p.x - x) < minDx && Math.abs(p.y - y) < minDy) return false
    }
    for (const p of planned) {
      if (Math.abs(p.x - x) < minDx && Math.abs(p.y - y) < minDy) return false
    }
    return true
  }

  const totalH  = (count - 1) * spreadY
  const startY  = parentPos.y - totalH / 2

  for (let i = 0; i < count; i++) {
    const preferredY = startY + i * spreadY
    let chosenY = preferredY
    let found   = false
    for (let step = 0; step <= 200 && !found; step++) {
      const dir  = step % 2 === 0 ? 1 : -1
      const k    = Math.ceil(step / 2)
      const candidate = preferredY + dir * k * minDy
      if (isFree(laneX, candidate)) {
        chosenY = candidate
        found   = true
      }
    }
    planned.push({ x: laneX, y: chosenY })
    allOccupied.push({ x: laneX, y: chosenY })
  }

  // Re-center the group around parent Y
  planned.sort((a, b) => a.y - b.y)
  const center = planned.reduce((s, p) => s + p.y, 0) / Math.max(1, planned.length)
  const shift  = parentPos.y - center
  return planned.map(p => ({ x: p.x, y: p.y + shift }))
}

// ─── [F5] RESOLVE COLLISIONS — iterative repulsion pass ──────────────────────
function resolveCollisions(newNodes, existingNodes, opts = {}) {
  if (!newNodes.length) return newNodes
  const minDist   = opts.minDist ?? 180
  const minDistSq = minDist * minDist
  const staticPts = existingNodes.map(n => ({ x: n.position.x, y: n.position.y }))
  const moving    = newNodes.map(n => ({
    id: n.id, x: n.position.x, y: n.position.y, laneX: n._laneX ?? n.position.x,
  }))

  for (let iter = 0; iter < 32; iter++) {
    let moved = false
    for (let i = 0; i < moving.length; i++) {
      const a = moving[i]
      let fx = 0, fy = 0

      for (let j = 0; j < moving.length; j++) {
        if (i === j) continue
        const b  = moving[j]
        const dx = a.x - b.x
        const dy = a.y - b.y
        const d2 = dx * dx + dy * dy || 0.01
        if (d2 < minDistSq) {
          const d    = Math.sqrt(d2)
          const push = (minDist - d) * 0.26
          fx += (dx / d) * push
          fy += (dy / d) * push
        }
      }
      for (const s of staticPts) {
        const dx = a.x - s.x
        const dy = a.y - s.y
        const d2 = dx * dx + dy * dy || 0.01
        if (d2 < minDistSq) {
          const d    = Math.sqrt(d2)
          const push = (minDist - d) * 0.32
          fx += (dx / d) * push
          fy += (dy / d) * push
        }
      }

      // Keep lane X fixed to prevent "messy" cross-lane drift.
      // We still use repulsion to adjust Y for overlap avoidance.
      const yBefore = a.y
      a.x = a.laneX
      a.y += fy
      if (Math.abs(a.y - yBefore) > 0.01) moved = true
    }
    if (!moved) break
  }

  return newNodes.map(n => {
    const m = moving.find(p => p.id === n.id)
    return m ? { ...n, position: { x: m.x, y: m.y } } : n
  })
}

// ─── [F7] BUILD ACTIVE PATH — root → clicked node ────────────────────────────
function buildActivePath(nodeId, parentByChildRef, rootIdsRef) {
  if (!nodeId) return []
  const path = []
  const seen  = new Set()
  let cur     = nodeId
  while (cur && !seen.has(cur)) {
    path.push(cur)
    if (rootIdsRef.has(cur)) break
    seen.add(cur)
    cur = parentByChildRef.get(cur)
  }
  return path.reverse()
}

// ─── [F7] COLLECT DESCENDANTS ────────────────────────────────────────────────
function collectDescendants(startId, childrenRef) {
  const out   = new Set()
  const stack = [...(childrenRef.get(startId) || [])]
  while (stack.length) {
    const id = stack.pop()
    if (out.has(id)) continue
    out.add(id)
    const kids = childrenRef.get(id)
    if (kids && kids.size) stack.push(...kids)
  }
  return out
}

// ─── [F7] COMPUTE FULL FLOW PATH for a node ──────────────────────────────────
// Returns all node IDs in the chain: ancestors + node + descendants
function computeFullFlowPath(nodeId, parentByChildRef, childrenByParentRef, rootIdsRef) {
  const path = buildActivePath(nodeId, parentByChildRef, rootIdsRef)
  const descendants = collectDescendants(nodeId, childrenByParentRef)
  return new Set([...path, nodeId, ...descendants])
}

// ─── NEIGHBOR SET (1-hop / 2-hop) ────────────────────────────────────────────
function computeNeighbors(focusedId, edges, twoHop = false) {
  if (!focusedId) return null
  const adj = new Map()
  edges.forEach(e => {
    if (!adj.has(e.source)) adj.set(e.source, new Set())
    if (!adj.has(e.target)) adj.set(e.target, new Set())
    adj.get(e.source).add(e.target)
    adj.get(e.target).add(e.source)
  })
  const seen     = new Set([focusedId])
  const q        = [{ id: focusedId, d: 0 }]
  const maxDepth = twoHop ? 2 : 1
  while (q.length) {
    const cur = q.shift()
    if (cur.d >= maxDepth) continue
    for (const n of (adj.get(cur.id) || [])) {
      if (!seen.has(n)) { seen.add(n); q.push({ id: n, d: cur.d + 1 }) }
    }
  }
  return seen
}

// ─── [F8] PARSE ENTITY REFS ──────────────────────────────────────────────────
function parseEntityRefs(text) {
  const refs    = new Set()
  const re      = /\b(\d{6,12})\b/g
  let m
  while ((m = re.exec(text)) !== null) refs.add(m[1])
  return refs
}

// ─── [F9] HOVER TOOLTIP ──────────────────────────────────────────────────────
function HoverTooltip({ nodeId, nodeData, position }) {
  const [meta, setMeta] = useState(null)
  const color           = C[nodeData?.type] || '#6b7280'

  useEffect(() => {
    if (!nodeId) return
    setMeta(null)
    // Use cached metadata first, then optionally fetch full record
    if (nodeData?.metadata && Object.keys(nodeData.metadata).length) {
      setMeta({ properties: nodeData.metadata })
    }
    axios.get(`${API}/api/graph/node/${nodeId}`)
      .then(r => setMeta(r.data))
      .catch(() => {})
  }, [nodeId, nodeData?.metadata])

  if (!nodeId || !position) return null

  const left = Math.min(position.x + 18, window.innerWidth - 300)
  const top  = Math.max(position.y - 24, 8)

  return (
    <div style={{
      position:     'fixed', left, top, zIndex: 999,
      width:        '268px',
      background:   '#ffffff',
      border:       `1px solid ${color}28`,
      borderTop:    `3px solid ${color}`,
      borderRadius: '10px',
      boxShadow:    '0 10px 30px rgba(0,0,0,0.14), 0 1px 4px rgba(0,0,0,0.06)',
      fontFamily:   'JetBrains Mono, monospace',
      pointerEvents:'none',
      animation:    'fadeIn 0.12s ease',
    }}>
      <div style={{ padding: '9px 14px 7px', borderBottom: '1px solid #eef2f8' }}>
        <div style={{
          fontSize: '9px', fontWeight: 700, letterSpacing: '0.1em',
          textTransform: 'uppercase', color,
          fontFamily: 'Syne, sans-serif',
        }}>
          {nodeData?.type}
        </div>
        <div style={{
          fontSize: '12px', color: '#1f2a37', marginTop: '2px', fontWeight: 600,
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }}>
          {nodeData?.label}
        </div>
      </div>
      <div style={{ padding: '8px 14px 10px' }}>
        {!meta ? (
          <div style={{ color: '#aab4c0', fontSize: '10px' }}>Loading…</div>
        ) : meta.properties && Object.keys(meta.properties).length ? (
          <>
            {Object.entries(meta.properties).slice(0, 7).map(([k, v]) => (
              <div key={k} style={{ display: 'flex', justifyContent: 'space-between', gap: '8px', marginBottom: '4px' }}>
                <span style={{ fontSize: '9px', color: '#8a96a6', textTransform: 'uppercase', letterSpacing: '0.05em', flexShrink: 0 }}>
                  {k.replace(/_/g, ' ')}
                </span>
                <span style={{ fontSize: '10px', color: '#2d3a4a', textAlign: 'right', wordBreak: 'break-word' }}>
                  {String(v)}
                </span>
              </div>
            ))}
            {Object.keys(meta.properties).length > 7 && (
              <div style={{ fontSize: '9px', color: '#aab4c0', fontStyle: 'italic', marginTop: '3px' }}>
                +{Object.keys(meta.properties).length - 7} more — click to inspect
              </div>
            )}
          </>
        ) : (
          <div style={{ color: '#aab4c0', fontSize: '10px' }}>No metadata</div>
        )}
      </div>
    </div>
  )
}

// ─── O2C NODE ─────────────────────────────────────────────────────────────────
function O2CNode({ data }) {
  const color     = C[data.type] || '#6b7280'
  const expanded  = data.expanded
  const hasKids   = data.hasChildren !== false

  // [F4] Fade logic
  const opacity   = data.dimmed ? 0.15 : 1
  const scale     = data.focused ? 1.07 : (data.pathNode ? 1.03 : 1)
  const showLabel = data.zoom === undefined || data.zoom >= LABEL_ZOOM_THRESHOLD

  // [F7] glow for full-path nodes
  let outlineStyle = 'none'
  if (data.chatHighlight)  outlineStyle = '2.5px solid #facc15'
  else if (data.focused)   outlineStyle = `2.5px solid ${color}`
  else if (data.pathNode)  outlineStyle = `1.5px solid ${color}88`

  return (
    <div
      onClick={data.onExpand}
      onMouseEnter={data.onHover}
      onMouseLeave={data.onHoverEnd}
      title={data.label}
      style={{
        position:   'relative',
        background: data.chatHighlight ? '#fef9c3' : `${color}18`,
        border:     `1.5px solid ${expanded ? color : `${color}55`}`,
        outline:    outlineStyle,
        outlineOffset: '2px',
        borderRadius: '8px',
        padding:    '9px 14px 9px 11px',
        minWidth:   '160px',
        maxWidth:   '230px',
        cursor:     hasKids ? 'pointer' : 'default',
        boxShadow:  data.focused
          ? `0 0 0 5px ${color}28, 0 6px 20px rgba(0,0,0,0.18)`
          : '0 2px 8px rgba(0,0,0,0.09)',
        transition: 'opacity 0.22s ease, transform 0.15s ease, box-shadow 0.15s ease',
        userSelect: 'none',
        opacity,
        transform:  `scale(${scale})`,
      }}
    >
      <Handle type="target" position={Position.Left}
        style={{ background: color, border: 'none', width: 6, height: 6 }} />

      <div style={{
        fontSize: '9px', fontWeight: 700, letterSpacing: '0.1em',
        textTransform: 'uppercase', color,
        marginBottom: showLabel ? '3px' : 0,
        fontFamily: 'Syne, sans-serif', opacity: 0.85,
      }}>
        {data.type}
      </div>

      {showLabel && (
        <div style={{
          fontSize: '11px', color: '#1f2a37',
          fontFamily: 'JetBrains Mono, monospace',
          lineHeight: 1.35,
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }}>
          {data.label}
        </div>
      )}

      {hasKids && (
        <div style={{
          position: 'absolute', top: '50%', right: '-10px',
          transform: 'translateY(-50%)',
          width: '18px', height: '18px', borderRadius: '50%',
          background: expanded ? color : '#ffffff',
          border: `1.5px solid ${color}`,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: '10px', color: expanded ? '#ffffff' : color,
          fontWeight: 700, zIndex: 10,
          transition: 'background 0.15s',
        }}>
          {expanded ? '−' : '+'}
        </div>
      )}

      <Handle type="source" position={Position.Right}
        style={{ background: color, border: 'none', width: 6, height: 6, right: -3 }} />
    </div>
  )
}

const nodeTypes = { o2c: O2CNode }

// ─── [F4] INSPECT PANEL ──────────────────────────────────────────────────────
function InspectPanel({ node, onClose }) {
  const [meta, setMeta]       = useState(null)
  const [loading, setLoading] = useState(true)
  const color                 = C[node?.data?.type] || '#6b7280'

  useEffect(() => {
    if (!node) return
    setLoading(true)
    setMeta(null)
    axios.get(`${API}/api/graph/node/${node.id}`)
      .then(r => setMeta(r.data))
      .catch(() => setMeta(null))
      .finally(() => setLoading(false))
  }, [node?.id])

  if (!node) return null

  return (
    <div style={{
      position:     'absolute', top: 16, right: 16, zIndex: 20,
      width:        '260px', background: '#ffffff',
      border:       `1px solid ${color}40`, borderTop: `3px solid ${color}`,
      borderRadius: '10px', boxShadow: '0 8px 24px rgba(0,0,0,0.14)',
      fontFamily:   'JetBrains Mono, monospace', overflow: 'hidden',
    }}>
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '10px 14px 8px', borderBottom: '1px solid #eef2f8',
      }}>
        <div>
          <div style={{ fontSize: '9px', fontWeight: 700, letterSpacing: '0.1em', textTransform: 'uppercase', color, fontFamily: 'Syne, sans-serif' }}>
            {node.data.type}
          </div>
          <div style={{ fontSize: '12px', color: '#1f2a37', marginTop: '2px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '190px', fontWeight: 600 }}>
            {node.data.label}
          </div>
        </div>
        <button onClick={onClose} style={{ background: 'none', border: 'none', color: '#aab4c0', cursor: 'pointer', fontSize: '18px', lineHeight: 1 }}>×</button>
      </div>
      <div style={{ padding: '10px 14px 12px' }}>
        {loading ? (
          <div style={{ color: '#aab4c0', fontSize: '11px' }}>Loading…</div>
        ) : meta?.properties ? (
          Object.entries(meta.properties).map(([k, v]) => (
            <div key={k} style={{ display: 'flex', justifyContent: 'space-between', gap: '8px', marginBottom: '6px', alignItems: 'flex-start' }}>
              <span style={{ fontSize: '9px', color: '#8a96a6', textTransform: 'uppercase', letterSpacing: '0.06em', flexShrink: 0 }}>
                {k.replace(/_/g, ' ')}
              </span>
              <span style={{ fontSize: '11px', color: '#1f2a37', textAlign: 'right', wordBreak: 'break-word' }}>
                {String(v)}
              </span>
            </div>
          ))
        ) : (
          <div style={{ color: '#aab4c0', fontSize: '11px' }}>No metadata</div>
        )}
      </div>
    </div>
  )
}

// ─── INNER GRAPH ──────────────────────────────────────────────────────────────
function GraphInner({ highlightChatRef }) {
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [loading, setLoading]            = useState(true)
  const [error, setError]                = useState(null)
  const [selected, setSelected]          = useState(null)

  // [F3] Focus state
  const [activeNodeId, setActiveNodeId]  = useState(null)
  const [activePath, setActivePath]      = useState([])    // full chain Set of ids
  const [focusedId, setFocusedId]        = useState(null)  // single clicked node id
  const [focusTwoHop, setFocusTwoHop]    = useState(false)

  // [F4] Fade state
  const [fadedNodes, setFadedNodes]      = useState(new Set())

  // [F8] Chat highlight
  const [highlightedNodes, setHighlightedNodes] = useState(new Set())

  // [F9] Hover
  const [hoveredId, setHoveredId]   = useState(null)
  const [hoverPos, setHoverPos]     = useState(null)
  const [hoverData, setHoverData]   = useState(null)

  // [F6] zoom tracking
  const [zoom, setZoom] = useState(1)

  // Refs
  const expandedRef          = useRef(new Set())
  const rootIdsRef           = useRef(new Set())   // [F1] never remove these
  const parentByChildRef     = useRef(new Map())
  const childrenByParentRef  = useRef(new Map())
  const depthsRef            = useRef(new Map())
  const hoverShowTimerRef    = useRef(null)
  const hoverHideTimerRef    = useRef(null)

  const { setCenter, fitBounds, getNodes, getViewport } = useReactFlow()

  // ── [F8] Chat integration ─────────────────────────────────────────────────
  useEffect(() => {
    if (!highlightChatRef) return
    highlightChatRef.current = (text) => {
      const refs = parseEntityRefs(text)
      const matchedNodes = nodes.filter(n => refs.has(n.id) || refs.has(String(n.id)))
      if (!matchedNodes.length) return

      // [F8] Highlight complete flow chain for each matched node:
      // ancestors + node + descendants (within the currently expanded graph).
      const flowIds = new Set()
      for (const n of matchedNodes) {
        const fp = computeFullFlowPath(
          n.id,
          parentByChildRef.current,
          childrenByParentRef.current,
          rootIdsRef.current
        )
        for (const id of fp) flowIds.add(id)
      }

      const flowNodes = nodes.filter(n => flowIds.has(n.id))
      if (!flowNodes.length) return

      setHighlightedNodes(flowIds)

      // [F6] Smooth zoom — no aggressive snap
      if (flowNodes.length === 1) {
        const n = flowNodes[0]
        const vp = getViewport()
        const targetZoom = Math.min(1.4, Math.max(vp.zoom, 0.8))
        setCenter(n.position.x + 110, n.position.y + 20, { zoom: targetZoom, duration: 700 })
      } else {
        const minX = Math.min(...flowNodes.map(n => n.position.x)) - 160
        const minY = Math.min(...flowNodes.map(n => n.position.y)) - 120
        const maxX = Math.max(...flowNodes.map(n => n.position.x + 240)) + 160
        const maxY = Math.max(...flowNodes.map(n => n.position.y + 80))  + 120
        fitBounds(
          { x: minX, y: minY, width: maxX - minX, height: maxY - minY },
          { duration: 700, padding: 0.25, maxZoom: 1.5 }
        )
      }

      setTimeout(() => setHighlightedNodes(new Set()), 7000)
    }
  }, [highlightChatRef, nodes, edges, setCenter, fitBounds, getViewport])

  // ── Node / Edge factories ─────────────────────────────────────────────────
  const makeRfNode = useCallback((raw, pos) => ({
    id:       raw.id,
    type:     'o2c',
    position: pos,
    data: {
      label:    raw.label,
      type:     raw.type,
      expanded: false,
      metadata: raw.metadata || {},
      hasChildren: !['payment', 'customer_company', 'sales_area', 'address', 'schedule_line', 'plant'].includes(raw.type),
    },
    draggable: true,
  }), [])

  const makeRfEdge = useCallback((raw, idx) => ({
    id:     `e-${raw.source}-${raw.target}-${idx}`,
    source: raw.source,
    target: raw.target,
    label:  LABELS[raw.relation] || raw.relation,
    type:   'smoothstep',
    pathOptions: { borderRadius: 28 },
    labelStyle:  { fontFamily: 'JetBrains Mono, monospace', fontSize: 9, fill: '#9aabb8' },
    labelBgStyle: { fill: '#ffffff', fillOpacity: 0.85 },
    labelBgPadding: [3, 4],
    style:  { stroke: '#b8cfe8', strokeWidth: 1.3, opacity: 0.15 },
    markerEnd: { type: MarkerType.ArrowClosed, width: 9, height: 9, color: '#b8cfe8' },
  }), [])

  // ── [F1] Load seed — root nodes are stored in rootIdsRef ─────────────────
  const loadSeed = useCallback(async () => {
    setLoading(true); setError(null)
    expandedRef.current.clear()
    setFocusedId(null); setActiveNodeId(null)
    setActivePath([]); setFadedNodes(new Set()); setHighlightedNodes(new Set())
    try {
      const { data } = await axios.get(`${API}/api/graph`)
      depthsRef.current.clear()
      parentByChildRef.current.clear()
      childrenByParentRef.current.clear()
      // [F1] Mark every seed node as a root
      rootIdsRef.current = new Set(data.nodes.map(n => n.id))
      data.nodes.forEach(n => depthsRef.current.set(n.id, 0))
      setNodes(data.nodes.map((n, i) => makeRfNode(n, { x: getLaneX(n.type), y: i * 100 + 60 })))
      setEdges([])
      setSelected(null)
    } catch { setError('Cannot reach backend at localhost:8000') }
    finally   { setLoading(false) }
  }, [makeRfNode, setNodes, setEdges])

  useEffect(() => { loadSeed() }, [loadSeed])

  // ── [F2] Expand node — only immediate neighbours, no duplicates ───────────
  const expandNode = useCallback(async (nodeId) => {
    // Toggle collapse
    if (expandedRef.current.has(nodeId)) {
      expandedRef.current.delete(nodeId)
      const descendants = collectDescendants(nodeId, childrenByParentRef.current)
      // [F1] NEVER remove root nodes even during collapse
      const removeIds = new Set([...descendants].filter(id => !rootIdsRef.current.has(id)))

      setEdges(prev => prev.filter(e =>
        !removeIds.has(e.source) && !removeIds.has(e.target) && e.source !== nodeId
      ))
      setNodes(prev =>
        prev
          // [F1] Filter out non-roots descendants only
          .filter(n => !removeIds.has(n.id))
          .map(n => n.id === nodeId ? { ...n, data: { ...n.data, expanded: false } } : n)
      )
      descendants.forEach(id => {
        if (!rootIdsRef.current.has(id)) {
          expandedRef.current.delete(id)
          depthsRef.current.delete(id)
          parentByChildRef.current.delete(id)
          childrenByParentRef.current.delete(id)
        }
      })
      const childSet = childrenByParentRef.current.get(nodeId)
      if (childSet) childSet.clear()

      setActiveNodeId(nodeId)
      setActivePath(buildActivePath(nodeId, parentByChildRef.current, rootIdsRef.current))
      if (focusedId === nodeId) setFocusedId(null)

      // [F6] Gentle zoom to parent after collapse
      setTimeout(() => {
        const live = getNodes()
        const parentNode = live.find(n => n.id === nodeId)
        if (!parentNode) return
        const vp = getViewport()
        const targetZoom = Math.min(1.2, Math.max(vp.zoom, 0.6))
        fitBounds(
          { x: parentNode.position.x - 200, y: parentNode.position.y - 160, width: 480, height: 340 },
          { duration: 480, padding: 0.3, maxZoom: targetZoom }
        )
      }, 0)
      return
    }

    // ── Expand: fetch and place immediate children only ───────────────────
    try {
      const { data } = await axios.get(`${API}/api/graph/expand/${nodeId}`)
      if (!data.nodes.length) return

      setActiveNodeId(nodeId)
      setActivePath(buildActivePath(nodeId, parentByChildRef.current, rootIdsRef.current))
      expandedRef.current.add(nodeId)

      setNodes(prev => {
        const parent      = prev.find(n => n.id === nodeId)
        const parentDepth = depthsRef.current.get(nodeId) ?? 0
        const existingIds = new Set(prev.map(n => n.id))

        // [F2] Skip already-present nodes
        const incoming = data.nodes.filter(n => !existingIds.has(n.id))
        if (!incoming.length) {
          return prev.map(n => n.id === nodeId ? { ...n, data: { ...n.data, expanded: true } } : n)
        }

        // Group by lane
        const laneGroups = new Map()
        incoming.forEach(raw => {
          const depth = parentDepth + 1
          depthsRef.current.set(raw.id, depth)
          const laneX = getLaneX(raw.type)
          if (!laneGroups.has(laneX)) laneGroups.set(laneX, [])
          laneGroups.get(laneX).push(raw)
        })

        const laneOrder       = [...laneGroups.keys()].sort((a, b) => a - b)
        const provisionalById = new Map()
        let occupied          = [...prev]

        laneOrder.forEach(laneX => {
          const laneChildren = laneGroups.get(laneX)
          const positions    = placeChildren(
            parent?.position || { x: laneX, y: 0 },
            laneChildren, occupied, { laneX, spreadY: 112 }
          )
          laneChildren.forEach((child, idx) => {
            provisionalById.set(child.id, { laneX, position: positions[idx] })
          })
          const provisionalNodes = laneChildren.map(child => ({
            id: child.id, position: provisionalById.get(child.id).position,
          }))
          occupied = [...occupied, ...provisionalNodes]
        })

        const newNodesRaw = incoming.map(raw => {
          const p    = provisionalById.get(raw.id)
          const node = makeRfNode(raw, p.position)
          node._laneX = p.laneX
          return node
        })

        const cleanedNewNodes = resolveCollisions(newNodesRaw, prev, { minDist: 210 }).map(n => {
          const clone = { ...n }
          delete clone._laneX
          return clone
        })

        return [
          ...prev.map(n => n.id === nodeId ? { ...n, data: { ...n.data, expanded: true } } : n),
          ...cleanedNewNodes,
        ]
      })

      setEdges(prev => {
        const existing = new Set(prev.map(e => e.id))
        data.edges.forEach(e => {
          if (e.source === nodeId) {
            parentByChildRef.current.set(e.target, nodeId)
            if (!childrenByParentRef.current.has(nodeId))
              childrenByParentRef.current.set(nodeId, new Set())
            childrenByParentRef.current.get(nodeId).add(e.target)
          }
        })
        return [
          ...prev,
          ...data.edges.map((e, i) => makeRfEdge(e, i)).filter(e => !existing.has(e.id)),
        ]
      })

      // [F6] Smooth, bounded zoom to newly expanded region
      const incomingIds = data.nodes.map(n => n.id)
      setTimeout(() => {
        const live    = getNodes()
        const targets = live.filter(n => n.id === nodeId || incomingIds.includes(n.id))
        if (!targets.length) return
        const minX = Math.min(...targets.map(n => n.position.x)) - 140
        const minY = Math.min(...targets.map(n => n.position.y)) - 120
        const maxX = Math.max(...targets.map(n => n.position.x + 240)) + 140
        const maxY = Math.max(...targets.map(n => n.position.y + 90))  + 120
        fitBounds(
          { x: minX, y: minY, width: maxX - minX, height: maxY - minY },
          { duration: 580, padding: 0.22, maxZoom: 1.4 }
        )
      }, 0)
    } catch {}
  }, [makeRfNode, makeRfEdge, setNodes, setEdges, focusedId, getNodes, fitBounds, getViewport])

  // ── [F7] Full-flow path (ancestors + node + descendants) ─────────────────
  const activeFlowPath = useMemo(() => {
    if (!activeNodeId) return new Set()
    return computeFullFlowPath(
      activeNodeId,
      parentByChildRef.current,
      childrenByParentRef.current,
      rootIdsRef.current
    )
  }, [activeNodeId])

  // ── Neighbor set for focus mode ───────────────────────────────────────────
  const neighborSet = useMemo(
    () => computeNeighbors(focusedId, edges, focusTwoHop),
    [focusedId, edges, focusTwoHop]
  )

  // ── [F4] Compute which nodes are faded ────────────────────────────────────
  // A node is faded only when something is focused AND it is out of the active path
  useEffect(() => {
    if (!activeNodeId && !focusedId) {
      setFadedNodes(new Set())
      return
    }
    const keep = new Set()
    // [F1] Root nodes are ALWAYS kept visible
    rootIdsRef.current.forEach(id => keep.add(id))

    // Full flow chain stays visible
    activeFlowPath.forEach(id => keep.add(id))

    // Neighbour set (1/2-hop) around focusedId
    if (neighborSet) neighborSet.forEach(id => keep.add(id))

    // Chat-highlighted nodes always visible
    highlightedNodes.forEach(id => keep.add(id))

    setFadedNodes(new Set(nodes.filter(n => !keep.has(n.id)).map(n => n.id)))
  }, [nodes, activeNodeId, focusedId, activeFlowPath, neighborSet, highlightedNodes])

  // ── Zoom tracking ─────────────────────────────────────────────────────────
  const onMove = useCallback((_, vp) => setZoom(vp.zoom), [])

  // ── Node click ────────────────────────────────────────────────────────────
  const onNodeClick = useCallback((evt, node) => {
    evt.stopPropagation()
    setSelected(node)
    setActiveNodeId(node.id)
    setActivePath(buildActivePath(node.id, parentByChildRef.current, rootIdsRef.current))
    setFocusedId(prev => prev === node.id ? null : node.id)
  }, [])

  const onPaneClick = useCallback(() => {
    setFocusedId(null)
    setSelected(null)
    setActiveNodeId(null)
    setActivePath([])
    setFadedNodes(new Set())
  }, [])

  // ── Hover handlers ────────────────────────────────────────────────────────
  const handleHover = useCallback((nodeId, label, type, metadata, evt) => {
    if (hoverHideTimerRef.current) clearTimeout(hoverHideTimerRef.current)
    if (hoverShowTimerRef.current) clearTimeout(hoverShowTimerRef.current)
    const { clientX: x, clientY: y } = evt
    hoverShowTimerRef.current = setTimeout(() => {
      setHoveredId(nodeId)
      setHoverData({ label, type, metadata })
      setHoverPos({ x, y })
    }, 90)
  }, [])

  const handleHoverEnd = useCallback(() => {
    if (hoverShowTimerRef.current) clearTimeout(hoverShowTimerRef.current)
    hoverHideTimerRef.current = setTimeout(() => {
      setHoveredId(null); setHoverPos(null); setHoverData(null)
    }, 80)
  }, [])

  useEffect(() => () => {
    if (hoverShowTimerRef.current) clearTimeout(hoverShowTimerRef.current)
    if (hoverHideTimerRef.current) clearTimeout(hoverHideTimerRef.current)
  }, [])

  // ── [F10] Compose nodes with handlers — memoized ──────────────────────────
  const nodesWithHandlers = useMemo(() =>
    nodes.map(n => ({
      ...n,
      data: {
        ...n.data,
        zoom,
        // [F4] Fade: 0.15 opacity for non-path nodes when focus is active
        dimmed:        fadedNodes.has(n.id),
        focused:       n.id === focusedId || n.id === activeNodeId,
        // [F7] pathNode = any node in the full ancestor+descendant chain
        pathNode:      activeFlowPath.has(n.id),
        chatHighlight: highlightedNodes.has(n.id),
        onExpand:  (e) => { e?.stopPropagation?.(); expandNode(n.id) },
        onHover:   (e) => handleHover(n.id, n.data.label, n.data.type, n.data.metadata, e),
        onHoverEnd: handleHoverEnd,
      },
    })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [nodes, fadedNodes, focusedId, activeNodeId, activeFlowPath, highlightedNodes, zoom, expandNode, handleHover, handleHoverEnd]
  )

  // ── [F7][F8] Compose edges — memoized ─────────────────────────────────────
  const edgesWithStyle = useMemo(() =>
    edges.map(e => {
      const bothInPath  = activeFlowPath.has(e.source) && activeFlowPath.has(e.target)
      const bothChat    = highlightedNodes.has(e.source) && highlightedNodes.has(e.target)
      const eitherFaded = fadedNodes.has(e.source) || fadedNodes.has(e.target)
      const inNeighbour = neighborSet
        ? (neighborSet.has(e.source) && neighborSet.has(e.target))
        : true

      let opacity, stroke, strokeWidth

      if (bothChat) {
        opacity = 0.98; stroke = '#facc15'; strokeWidth = 2.6
      } else if (bothInPath) {
        // [F7] Full flow edges highlighted
        opacity = 0.92; stroke = '#2f80ed'; strokeWidth = 2.2
      } else if (eitherFaded) {
        // [F4] Faded edges
        opacity = 0.08; stroke = '#b8cfe8'; strokeWidth = 0.8
      } else if (inNeighbour) {
        opacity = 0.65; stroke = '#5da0d8'; strokeWidth = 1.4
      } else {
        opacity = 0.15; stroke = '#b8cfe8'; strokeWidth = 0.8
      }

      const curveOffset = 20 + (e.id.length % 5) * 7
      return {
        ...e,
        type: 'smoothstep',
        pathOptions: { borderRadius: 26, offset: curveOffset },
        style: {
          ...e.style,
          stroke, strokeWidth, opacity,
          transition: 'stroke 180ms ease, opacity 200ms ease',
        },
        markerEnd: { ...e.markerEnd, color: stroke },
      }
    }),
    [edges, activeFlowPath, highlightedNodes, fadedNodes, neighborSet]
  )

  // ── Legend counts ─────────────────────────────────────────────────────────
  const typeCounts = useMemo(() =>
    nodes.reduce((acc, n) => { acc[n.data.type] = (acc[n.data.type] || 0) + 1; return acc }, {}),
    [nodes]
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: '#f4f7fb' }}>
      {/* Header */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '12px 16px 10px', borderBottom: '1px solid #dde6f1',
        background: '#ffffff', flexShrink: 0,
      }}>
        <div>
          <div style={{ fontFamily: 'Syne, sans-serif', fontSize: '13px', color: '#9aa5b1' }}>
            Mapping / <span style={{ color: '#2f3d50', fontWeight: 700 }}>Order to Cash</span>
          </div>
          <div style={{ fontSize: '10px', color: '#7f8b99', marginTop: '2px' }}>
            {nodes.length} nodes · {edges.length} edges ·{' '}
            {focusedId
              ? <span style={{ color: '#3b82f6' }}>
                  focus mode {focusTwoHop ? '(2-hop)' : '(1-hop)'} — click pane to clear
                </span>
              : 'click + to expand · click node to focus flow'
            }
          </div>
        </div>
        <div style={{ display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap', maxWidth: '460px' }}>
          {Object.entries(typeCounts).map(([type, count]) => (
            <div key={type} style={{
              display: 'flex', alignItems: 'center', gap: '4px', padding: '2px 7px',
              borderRadius: '12px', background: `${C[type] || '#6b7280'}15`,
              border: `1px solid ${C[type] || '#6b7280'}40`,
            }}>
              <div style={{ width: '6px', height: '6px', borderRadius: '50%', background: C[type] || '#6b7280' }} />
              <span style={{ fontSize: '9px', color: C[type] || '#6b7280', fontFamily: 'Syne, sans-serif', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                {type}
              </span>
              <span style={{ fontSize: '9px', color: '#8a96a6', fontFamily: 'JetBrains Mono, monospace' }}>{count}</span>
            </div>
          ))}
          <button onClick={loadSeed} style={btnStyle}>↺ Reset</button>
        </div>
      </div>

      {/* Graph canvas */}
      <div style={{ flex: 1, position: 'relative' }}>
        {loading && (
          <div style={{ position: 'absolute', inset: 0, zIndex: 10, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#5f6f84', fontSize: '12px', background: '#f4f7fb' }}>
            Loading customers…
          </div>
        )}
        {!loading && error && (
          <div style={{ position: 'absolute', inset: 0, zIndex: 10, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: '10px' }}>
            <span style={{ color: '#b84444', fontSize: '12px' }}>{error}</span>
            <button onClick={loadSeed} style={{ ...btnStyle, borderColor: '#f5a0a0', color: '#b84444' }}>↺ Retry</button>
          </div>
        )}
        {!loading && !error && (
          <ReactFlow
            nodes={nodesWithHandlers}
            edges={edgesWithStyle}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={onNodeClick}
            onPaneClick={onPaneClick}
            onMove={onMove}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.22 }}
            minZoom={0.05}
            maxZoom={2.5}
            nodesDraggable
            nodesConnectable={false}
            panOnDrag
            zoomOnScroll
          >
            <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#d5e2f1" />
            <Controls showInteractive={false} />
          </ReactFlow>
        )}
        <InspectPanel node={selected} onClose={() => { setSelected(null); setFocusedId(null) }} />
        {hoveredId && hoveredId !== selected?.id && (
          <HoverTooltip nodeId={hoveredId} nodeData={hoverData} position={hoverPos} />
        )}
      </div>

      {/* Inline fade-in animation */}
      <style>{`@keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }`}</style>
    </div>
  )
}

// ─── PUBLIC EXPORT ────────────────────────────────────────────────────────────
export default function GraphView({ highlightChatRef }) {
  return (
    <ReactFlowProvider>
      <GraphInner highlightChatRef={highlightChatRef} />
    </ReactFlowProvider>
  )
}

// ─── Chat integration wiring (App.jsx) ───────────────────────────────────────
// const graphHighlightRef = useRef(null)
// <GraphView highlightChatRef={graphHighlightRef} />
// <Chat onAnswer={(text) => graphHighlightRef.current?.(text)} />

const btnStyle = {
  background:   '#ffffff',
  border:       '1px solid #d7e1ef',
  color:        '#445367',
  borderRadius: '8px',
  padding:      '6px 10px',
  cursor:       'pointer',
  fontFamily:   'var(--font-mono)',
  fontSize:     '10.5px',
  whiteSpace:   'nowrap',
}
