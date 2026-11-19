package hub

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
)

type pendingEvent struct {
	id     string
	handed bool
}

type progress struct {
	loaded       bool
	cursor, head int64
	generation   uint64
	order        []int64
	pending      map[int64]pendingEvent
	recent       [1024]string
	next         int
}

func (c *Client) loadCursor() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded {
		return nil
	}
	path, err := c.statePath("cursor.json")
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(path)
	var cursor int64
	if !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return err
		}
		if json.Unmarshal(raw, &cursor) != nil || cursor < 0 {
			return errors.New("invalid persisted peer cursor")
		}
	}
	c.progress = progress{loaded: true, cursor: cursor, head: cursor, pending: make(map[int64]pendingEvent)}
	return nil
}

func (c *Client) persist(seq int64) error {
	path, err := c.statePath("cursor.json")
	if err != nil {
		return err
	}
	raw, err := json.Marshal(seq)
	if err != nil {
		return err
	}
	if err = saveFile(path, raw, false); err != nil {
		return err
	}
	c.cursor = seq
	return nil
}

func (c *Client) resetCursor(seq int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq < 0 || seq >= c.cursor {
		return errors.New("invalid bad_cursor recovery sequence")
	}
	if err := c.persist(seq); err != nil {
		return err
	}
	c.progress = progress{loaded: true, cursor: seq, head: seq, generation: c.generation + 1, pending: make(map[int64]pendingEvent)}
	return nil
}

func (c *Client) advance() error {
	for len(c.order) > 0 {
		if _, ok := c.pending[c.order[0]]; ok {
			break
		}
		c.order = c.order[1:]
	}
	mark := c.head
	if len(c.order) > 0 {
		mark = c.order[0] - 1
	}
	if mark > c.cursor {
		return c.persist(mark)
	}
	return nil
}

type generationKey struct{}

func (c *Client) Ack(ctx context.Context, seq int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen, ok := ctx.Value(generationKey{}).(uint64); ok && gen != c.generation {
		return nil
	}
	if seq <= c.cursor {
		return nil
	}
	p, ok := c.pending[seq]
	if ok {
		delete(c.pending, seq)
		if p.id != "" {
			c.recent[c.next] = p.id
			c.next = (c.next + 1) % len(c.recent)
			for n, other := range c.pending {
				if other.id == p.id {
					delete(c.pending, n)
				}
			}
		}
	} else if seq > c.head {
		return errors.New("acknowledgement beyond observed peer events")
	}
	return c.advance()
}

func (c *Client) observe(seq int64, id string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, exists := c.pending[seq]
	if !exists && seq <= c.head || exists && p.handed {
		return false, c.advance()
	}
	if !exists {
		c.head = seq
		p.id = id
		if id != "" {
			if slices.Contains(c.recent[:], id) {
				return false, c.advance()
			}
			for _, other := range c.pending {
				if other.id == id && other.handed {
					p.handed = true
					break
				}
			}
		}
		c.order = append(c.order, seq)
	}
	dispatch := !p.handed
	p.handed = true
	c.pending[seq] = p
	if err := c.advance(); err != nil {
		p.handed = false
		c.pending[seq] = p
		return false, err
	}
	return dispatch, nil
}
