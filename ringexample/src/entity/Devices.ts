import {Entity, PrimaryGeneratedColumn, Column} from "typeorm";

@Entity()
export class Devices {

    @PrimaryGeneratedColumn()
    id: number;

    @Column()
    deviceid: string;

    @Column()
    devicename: string;

    @Column()
    buttonpush_extension: boolean;

    @Column()
    motion_extension: boolean;

    @Column()
    liveview_extension: boolean;

}
