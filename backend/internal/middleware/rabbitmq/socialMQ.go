package rabbitmq

import (
	"context"
	"errors"
	"time"
)

// SocialMQ 负责「关注/取关事件」的发布。
type SocialMQ struct {
	p *publisher
}

const (
	socialExchange   = "social.events"
	socialQueue      = "social.events"
	socialBindingKey = "social.*"

	socialFollowRK   = "social.follow"
	socialUnfollowRK = "social.unfollow"
)

type SocialEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	FollowerID uint      `json:"follower_id"`
	VloggerID  uint      `json:"vlogger_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewSocialMQ(base *RabbitMQ) (*SocialMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	if err := DeclareTopic(ch, socialExchange, socialQueue, socialBindingKey); err != nil {
		ch.Close()
		return nil, err
	}
	p, err := newPublisher(ch, DefaultConfirmTimeout)
	if err != nil {
		ch.Close()
		return nil, err
	}
	return &SocialMQ{p: p}, nil
}

func (s *SocialMQ) Close() error {
	if s == nil || s.p == nil {
		return nil
	}
	return s.p.Close()
}

func (s *SocialMQ) Follow(ctx context.Context, followerID, vloggerID uint) error {
	return s.publish(ctx, "follow", socialFollowRK, followerID, vloggerID)
}

func (s *SocialMQ) UnFollow(ctx context.Context, followerID, vloggerID uint) error {
	return s.publish(ctx, "unfollow", socialUnfollowRK, followerID, vloggerID)
}

func (s *SocialMQ) publish(ctx context.Context, action, routingKey string, followerID, vloggerID uint) error {
	if s == nil || s.p == nil {
		return errors.New("social mq is not initialized")
	}
	if followerID == 0 || vloggerID == 0 {
		return errors.New("followerID and vloggerID are required")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	evt := SocialEvent{
		EventID:    id,
		Action:     action,
		FollowerID: followerID,
		VloggerID:  vloggerID,
		OccurredAt: time.Now().UTC(),
	}
	return s.p.PublishJSON(ctx, socialExchange, routingKey, evt)
}
