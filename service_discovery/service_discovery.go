package servicediscovery

type Participant struct {
	ID  string `json:"id"`
	Url string `json:"url"`
}

type ServiceDiscovery interface {
	GetParticipants() []Participant
}
