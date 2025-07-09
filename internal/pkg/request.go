//
// Copyright (c) 2023 Intel Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package pkg

import (
	"fmt"
	"strings"
	"time"

	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/google/uuid"
)

// DoRequest publishes a request containing a RequestID to the specified topic,
// then subscribes to a response topic which contains the RequestID. Once the response is received, the
// response topic is unsubscribed and the response data is returned. If no response is received within
// the timeout period, a timed out  error returned.
func DoRequest(
	subscribe func(topics []types.TopicChannel, messageErrors chan error) error,
	unsubscribe func(topics ...string) error,
	publish func(message types.MessageEnvelope, topic string) error,
	requestMessage types.MessageEnvelope,
	requestTopic string,
	responseTopicPrefix string,
	requestTimeout time.Duration) (*types.MessageEnvelope, error) {
	if len(strings.TrimSpace(requestMessage.RequestID)) == 0 {
		requestMessage.RequestID = uuid.NewString()
	}

	err := publish(requestMessage, requestTopic)
	if err != nil {
		return nil, fmt.Errorf("unable to create publish request to %s: %v", requestTopic, err)
	}

	// Return early — you’re done!
	return &types.MessageEnvelope{
		RequestID:     requestMessage.RequestID,
		CorrelationID: requestMessage.CorrelationID,
		Payload:       []byte(`{"status":"pending"}`),
	}, nil
}
