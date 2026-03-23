import os
import multiprocessing as mp
from swarm.agent import Agent
from swarm.coordinator import Coordinator
from governance.protocol import GovernanceProtocol

def main():
    # Initialize the coordinator
    coordinator = Coordinator()

    # Spawn the agent swarm
    agents = [Agent(coordinator) for _ in range(mp.cpu_count())]
    for agent in agents:
        agent.start()

    # Run the governance protocol
    protocol = GovernanceProtocol(coordinator)
    protocol.run()

if __name__ == '__main__':
    main()