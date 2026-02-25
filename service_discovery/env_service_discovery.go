package servicediscovery

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type EnvServiceDiscovery struct {
	participants   []Participant
	participantMap map[string]*Participant
}

func NewEnvServiceDiscovery() *EnvServiceDiscovery {
	godotenv.Load()

	EnvServiceDiscovery := &EnvServiceDiscovery{}

	EnvServiceDiscovery.participants = getParticipants()

	EnvServiceDiscovery.participantMap = make(map[string]*Participant)

	for _, participant := range EnvServiceDiscovery.participants {
		EnvServiceDiscovery.participantMap[participant.ID] = &participant
	}

	return EnvServiceDiscovery
}

func getParticipants() []Participant {

	particpants := os.Getenv("PARTICIPANTS")

	if particpants == "" {
		panic("NO participants found in environment variables")
	}

	peers := strings.Split(particpants, ",")
	peersParsed := make([]Participant, len(peers))

	for i, peer := range peers {
		peersParsed[i] = Participant{
			Url: peer,
			ID:  peer,
		}
	}

	return peersParsed
}

func (e *EnvServiceDiscovery) GetParticipants() []Participant {
	return e.participants
}

func (e *EnvServiceDiscovery) GetUrlForParticipant(participantId string) string {
	return e.participantMap[participantId].Url
}
