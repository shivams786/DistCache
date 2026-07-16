package transport

import (
	"context"

	"google.golang.org/grpc"
)

type GetRequest struct {
	Key        string `json:"key"`
	OriginNode string `json:"origin_node,omitempty"`
	HopCount   int32  `json:"hop_count,omitempty"`
}

type GetResponse struct {
	Key               string `json:"key"`
	Value             []byte `json:"value,omitempty"`
	Found             bool   `json:"found"`
	ServedBy          string `json:"served_by"`
	ExpiresAtUnixNano int64  `json:"expires_at_unix_nano,omitempty"`
}

type SetRequest struct {
	Key               string `json:"key"`
	Value             []byte `json:"value"`
	TTLSeconds        int64  `json:"ttl_seconds,omitempty"`
	ExpiresAtUnixNano int64  `json:"expires_at_unix_nano,omitempty"`
	OriginNode        string `json:"origin_node,omitempty"`
	HopCount          int32  `json:"hop_count,omitempty"`
	SkipReplication   bool   `json:"skip_replication,omitempty"`
}

type SetResponse struct {
	Key               string `json:"key"`
	Stored            bool   `json:"stored"`
	PrimaryNode       string `json:"primary_node"`
	ReplicationQueued bool   `json:"replication_queued"`
}

type DeleteRequest struct {
	Key             string `json:"key"`
	OriginNode      string `json:"origin_node,omitempty"`
	HopCount        int32  `json:"hop_count,omitempty"`
	SkipReplication bool   `json:"skip_replication,omitempty"`
}

type DeleteResponse struct {
	Key               string `json:"key"`
	Deleted           bool   `json:"deleted"`
	PrimaryNode       string `json:"primary_node"`
	ReplicationQueued bool   `json:"replication_queued"`
}

type ReplicateRequest struct {
	Key               string `json:"key"`
	Value             []byte `json:"value"`
	ExpiresAtUnixNano int64  `json:"expires_at_unix_nano,omitempty"`
	SourceNode        string `json:"source_node"`
}

type ReplicateResponse struct {
	Key    string `json:"key"`
	Stored bool   `json:"stored"`
	NodeID string `json:"node_id"`
}

type HealthRequest struct {
	RequesterNode string `json:"requester_node,omitempty"`
}

type HealthResponse struct {
	NodeID       string `json:"node_id"`
	Healthy      bool   `json:"healthy"`
	CacheEntries int64  `json:"cache_entries"`
	RequestCount uint64 `json:"request_count"`
	TimeUnixNano int64  `json:"time_unix_nano"`
}

type SyncRequest struct {
	RequesterNode string `json:"requester_node"`
}

type SyncEntry struct {
	Key               string `json:"key"`
	Value             []byte `json:"value"`
	ExpiresAtUnixNano int64  `json:"expires_at_unix_nano,omitempty"`
}

type CacheNodeServer interface {
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Set(context.Context, *SetRequest) (*SetResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	Replicate(context.Context, *ReplicateRequest) (*ReplicateResponse, error)
	Health(context.Context, *HealthRequest) (*HealthResponse, error)
	Sync(*SyncRequest, CacheNode_SyncServer) error
}

type CacheNode_SyncServer interface {
	Send(*SyncEntry) error
	grpc.ServerStream
}

type CacheNodeClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
	Replicate(ctx context.Context, in *ReplicateRequest, opts ...grpc.CallOption) (*ReplicateResponse, error)
	Health(ctx context.Context, in *HealthRequest, opts ...grpc.CallOption) (*HealthResponse, error)
	Sync(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (CacheNode_SyncClient, error)
}

type CacheNode_SyncClient interface {
	Recv() (*SyncEntry, error)
	grpc.ClientStream
}

func RegisterCacheNodeServer(s grpc.ServiceRegistrar, srv CacheNodeServer) {
	s.RegisterService(&CacheNode_ServiceDesc, srv)
}

func NewCacheNodeClient(cc grpc.ClientConnInterface) CacheNodeClient {
	return &cacheNodeClient{cc}
}

type cacheNodeClient struct {
	cc grpc.ClientConnInterface
}

func defaultCallOptions(opts []grpc.CallOption) []grpc.CallOption {
	out := []grpc.CallOption{grpc.ForceCodec(JSONCodec{})}
	out = append(out, opts...)
	return out
}

func (c *cacheNodeClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/distcache.CacheNode/Get", in, out, defaultCallOptions(opts)...)
	return out, err
}

func (c *cacheNodeClient) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error) {
	out := new(SetResponse)
	err := c.cc.Invoke(ctx, "/distcache.CacheNode/Set", in, out, defaultCallOptions(opts)...)
	return out, err
}

func (c *cacheNodeClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, "/distcache.CacheNode/Delete", in, out, defaultCallOptions(opts)...)
	return out, err
}

func (c *cacheNodeClient) Replicate(ctx context.Context, in *ReplicateRequest, opts ...grpc.CallOption) (*ReplicateResponse, error) {
	out := new(ReplicateResponse)
	err := c.cc.Invoke(ctx, "/distcache.CacheNode/Replicate", in, out, defaultCallOptions(opts)...)
	return out, err
}

func (c *cacheNodeClient) Health(ctx context.Context, in *HealthRequest, opts ...grpc.CallOption) (*HealthResponse, error) {
	out := new(HealthResponse)
	err := c.cc.Invoke(ctx, "/distcache.CacheNode/Health", in, out, defaultCallOptions(opts)...)
	return out, err
}

func (c *cacheNodeClient) Sync(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (CacheNode_SyncClient, error) {
	stream, err := c.cc.NewStream(ctx, &CacheNode_ServiceDesc.Streams[0], "/distcache.CacheNode/Sync", defaultCallOptions(opts)...)
	if err != nil {
		return nil, err
	}
	client := &cacheNodeSyncClient{ClientStream: stream}
	if err := client.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := client.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return client, nil
}

type cacheNodeSyncClient struct {
	grpc.ClientStream
}

func (x *cacheNodeSyncClient) Recv() (*SyncEntry, error) {
	m := new(SyncEntry)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var CacheNode_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "distcache.CacheNode",
	HandlerType: (*CacheNodeServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Get", Handler: _CacheNode_Get_Handler},
		{MethodName: "Set", Handler: _CacheNode_Set_Handler},
		{MethodName: "Delete", Handler: _CacheNode_Delete_Handler},
		{MethodName: "Replicate", Handler: _CacheNode_Replicate_Handler},
		{MethodName: "Health", Handler: _CacheNode_Health_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "Sync", Handler: _CacheNode_Sync_Handler, ServerStreams: true},
	},
	Metadata: "proto/cache.proto",
}

func _CacheNode_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheNodeServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distcache.CacheNode/Get"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheNodeServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CacheNode_Set_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheNodeServer).Set(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distcache.CacheNode/Set"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheNodeServer).Set(ctx, req.(*SetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CacheNode_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheNodeServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distcache.CacheNode/Delete"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheNodeServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CacheNode_Replicate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplicateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheNodeServer).Replicate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distcache.CacheNode/Replicate"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheNodeServer).Replicate(ctx, req.(*ReplicateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CacheNode_Health_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HealthRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheNodeServer).Health(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distcache.CacheNode/Health"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheNodeServer).Health(ctx, req.(*HealthRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CacheNode_Sync_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SyncRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(CacheNodeServer).Sync(m, &cacheNodeSyncServer{ServerStream: stream})
}

type cacheNodeSyncServer struct {
	grpc.ServerStream
}

func (x *cacheNodeSyncServer) Send(m *SyncEntry) error {
	return x.ServerStream.SendMsg(m)
}
