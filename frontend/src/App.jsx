import { useRef } from 'react'
import GraphView from './components/GraphView.jsx'
import Chat from './components/Chat.jsx'

export default function App() {
  const graphHighlightRef = useRef(null)

  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: '1fr 360px',
      height: '100vh',
      width: '100vw',
      overflow: 'hidden',
      background: '#f4f7fb',
    }}>
      <div style={{
        overflow: 'hidden',
        borderRight: '1px solid #dde6f1',
        background: '#f4f7fb',
      }}>
        {/* ✅ PASS REF TO GRAPH */}
        <GraphView highlightChatRef={graphHighlightRef} />
      </div>

      <div style={{
        overflow: 'hidden',
        background: '#ffffff',
      }}>
        {/* ✅ CONNECT CHAT TO GRAPH */}
        <Chat
          onAnswer={(text) => {
            graphHighlightRef.current?.(text)
          }}
        />
      </div>
    </div>
  )
}