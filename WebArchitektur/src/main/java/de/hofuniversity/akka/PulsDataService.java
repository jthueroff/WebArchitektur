package de.hofuniversity.akka;

import javax.annotation.PostConstruct;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.SmallestMailboxRouter;
import de.hofuniversity.general.PulsData;

public class PulsDataService {
	ActorSystem sys;
	ActorRef ac;

	public PulsDataService() {
		
		sys = ActorSystem.create("pulsdata");
		ac = sys.actorOf(new Props(PulsDataActor.class).withRouter(new SmallestMailboxRouter(100)));
		sys.eventStream().subscribe(ac, PulsData.class);
	}
	
	@PostConstruct
	public void pulsDataArrived(PulsData data)
	{
		sys.eventStream().publish(data);
	}
}
